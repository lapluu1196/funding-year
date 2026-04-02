#!/usr/bin/env python3
import argparse
import concurrent.futures
import html as html_lib
import json
import logging
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
TAG = f"{{{NS_MAIN}}}"
CHECKPOINT_VERSION = 1

ET.register_namespace("", NS_MAIN)


@dataclass(frozen=True)
class ExtractedProjectData:
    funding_year: Optional[int] = None
    open_date: Optional[str] = None
    close_date: Optional[str] = None


@dataclass(frozen=True)
class SourceConfig:
    name: str
    financier_value: str
    fetcher: Callable[[str, float], str]
    extractor: Callable[[str], ExtractedProjectData]


@dataclass
class RunStats:
    scanned: int = 0
    matched_financier: int = 0
    fetched: int = 0
    updated: int = 0
    errors: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "scanned": self.scanned,
            "matched_financier": self.matched_financier,
            "fetched": self.fetched,
            "updated": self.updated,
            "errors": self.errors,
        }

    @staticmethod
    def from_dict(raw: Dict[str, int]) -> "RunStats":
        return RunStats(
            scanned=int(raw.get("scanned", 0)),
            matched_financier=int(raw.get("matched_financier", 0)),
            fetched=int(raw.get("fetched", 0)),
            updated=int(raw.get("updated", 0)),
            errors=int(raw.get("errors", 0)),
        )


@dataclass
class PendingFetch:
    data_row_pos: int
    row: ET.Element
    row_idx: int
    url: str
    future: concurrent.futures.Future


def col_to_index(col_letters: str) -> int:
    value = 0
    for ch in col_letters.upper():
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Invalid column letter: {col_letters}")
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


def index_to_col(index: int) -> str:
    if index <= 0:
        raise ValueError(f"Invalid column index: {index}")
    chars: List[str] = []
    while index > 0:
        index, rem = divmod(index - 1, 26)
        chars.append(chr(rem + ord("A")))
    return "".join(reversed(chars))


def split_ref(cell_ref: str) -> Tuple[str, int]:
    match = re.fullmatch(r"([A-Z]+)(\d+)", cell_ref)
    if not match:
        raise ValueError(f"Invalid cell reference: {cell_ref}")
    return match.group(1), int(match.group(2))


def load_xlsx_as_map(path: Path) -> Dict[str, bytes]:
    with zipfile.ZipFile(path, "r") as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def save_xlsx_from_map(path: Path, files: Dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)


def get_first_sheet_path(files: Dict[str, bytes]) -> str:
    candidates = sorted(
        name
        for name in files
        if name.startswith("xl/worksheets/") and name.endswith(".xml")
    )
    if not candidates:
        raise RuntimeError("No worksheet XML found in workbook.")
    return candidates[0]


def parse_shared_strings(files: Dict[str, bytes]) -> List[str]:
    if "xl/sharedStrings.xml" not in files:
        return []

    root = ET.fromstring(files["xl/sharedStrings.xml"])
    strings: List[str] = []
    for si in root.findall(f"{TAG}si"):
        text = "".join(node.text or "" for node in si.iter(f"{TAG}t"))
        strings.append(text)
    return strings


def get_cell_text(cell: ET.Element, shared_strings: List[str]) -> str:
    cell_type = cell.get("t")
    if cell_type == "s":
        value_node = cell.find(f"{TAG}v")
        if value_node is None or value_node.text is None:
            return ""
        index = int(value_node.text)
        return shared_strings[index] if 0 <= index < len(shared_strings) else ""

    if cell_type == "inlineStr":
        inline_node = cell.find(f"{TAG}is")
        if inline_node is None:
            return ""
        return "".join(node.text or "" for node in inline_node.iter(f"{TAG}t"))

    value_node = cell.find(f"{TAG}v")
    if value_node is None or value_node.text is None:
        return ""
    return value_node.text


def read_row_values(
    row: ET.Element, shared_strings: List[str]
) -> Dict[int, Tuple[str, ET.Element]]:
    values: Dict[int, Tuple[str, ET.Element]] = {}
    for cell in row.findall(f"{TAG}c"):
        cell_ref = cell.get("r")
        if not cell_ref:
            continue
        col_letters, _ = split_ref(cell_ref)
        col_idx = col_to_index(col_letters)
        values[col_idx] = (get_cell_text(cell, shared_strings), cell)
    return values


def find_or_create_cell(row: ET.Element, col_idx: int, row_idx: int) -> ET.Element:
    col_letters = index_to_col(col_idx)
    target_ref = f"{col_letters}{row_idx}"
    target: Optional[ET.Element] = None
    cells = row.findall(f"{TAG}c")

    for cell in cells:
        if cell.get("r") == target_ref:
            target = cell
            break

    if target is None:
        target = ET.Element(f"{TAG}c", {"r": target_ref})
        inserted = False
        for i, cell in enumerate(cells):
            current_ref = cell.get("r")
            if not current_ref:
                continue
            current_col, _ = split_ref(current_ref)
            if col_to_index(current_col) > col_idx:
                row.insert(i, target)
                inserted = True
                break
        if not inserted:
            row.append(target)

    return target


def clear_cell_content(target: ET.Element) -> None:
    for child in list(target):
        target.remove(child)


def set_numeric_cell(row: ET.Element, col_idx: int, row_idx: int, number: int) -> None:
    target = find_or_create_cell(row=row, col_idx=col_idx, row_idx=row_idx)
    target.attrib.pop("t", None)
    clear_cell_content(target)
    value_node = ET.SubElement(target, f"{TAG}v")
    value_node.text = str(number)


def set_text_cell(row: ET.Element, col_idx: int, row_idx: int, text: str) -> None:
    target = find_or_create_cell(row=row, col_idx=col_idx, row_idx=row_idx)
    target.attrib["t"] = "inlineStr"
    clear_cell_content(target)

    is_node = ET.SubElement(target, f"{TAG}is")
    t_node = ET.SubElement(is_node, f"{TAG}t")
    if text.strip() != text:
        t_node.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    t_node.text = text


def fetch_html(url: str, timeout: float) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (funding-year-filler/1.0)",
            "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def fetch_project_data_from_url(
    url: str, timeout: float, source: SourceConfig
) -> ExtractedProjectData:
    raw_payload = source.fetcher(url, timeout)
    return source.extractor(raw_payload)


def extract_balanced_div(html: str, div_start: int) -> Optional[str]:
    token_pattern = re.compile(r"<div\b|</div\s*>", re.IGNORECASE)
    depth = 0
    for match in token_pattern.finditer(html, pos=div_start):
        token = match.group(0).lower()
        if token.startswith("<div"):
            depth += 1
        else:
            depth -= 1
            if depth == 0:
                return html[div_start : match.end()]
    return None


def iter_arvsfonden_description_sections(html: str) -> Iterable[str]:
    marker = "arv-project-presentation__main-content-left-column"
    heading = "Beskrivning av projektet"
    seek_from = 0

    while True:
        heading_idx = html.find(heading, seek_from)
        if heading_idx == -1:
            return

        marker_idx = html.rfind(marker, 0, heading_idx)
        if marker_idx == -1:
            seek_from = heading_idx + len(heading)
            continue

        div_start = html.rfind("<div", 0, marker_idx)
        if div_start == -1:
            seek_from = heading_idx + len(heading)
            continue

        block = extract_balanced_div(html, div_start)
        if block:
            yield block

        seek_from = heading_idx + len(heading)


def extract_year_arvsfonden_from_description(html: str) -> Optional[int]:
    diary_pattern = re.compile(
        r"Diarienummer:\s*[^<\n\r]*?-\s*(19\d{2}|20\d{2})\b", re.IGNORECASE
    )
    for section in iter_arvsfonden_description_sections(html):
        match = diary_pattern.search(section)
        if match:
            return int(match.group(1))
    return None


def iter_arvsfonden_tidstatus_sections(html: str) -> Iterable[str]:
    marker = "arv-project-presentation__sum-and-progress-container"
    heading = "Projektets tidstatus"
    seek_from = 0

    while True:
        heading_idx = html.find(heading, seek_from)
        if heading_idx == -1:
            return

        marker_idx = html.rfind(marker, 0, heading_idx)
        if marker_idx == -1:
            seek_from = heading_idx + len(heading)
            continue

        div_start = html.rfind("<div", 0, marker_idx)
        if div_start == -1:
            seek_from = heading_idx + len(heading)
            continue

        block = extract_balanced_div(html, div_start)
        if block:
            yield block

        seek_from = heading_idx + len(heading)


SWEDISH_MONTHS: Dict[str, str] = {
    "januari": "01",
    "februari": "02",
    "mars": "03",
    "april": "04",
    "maj": "05",
    "juni": "06",
    "juli": "07",
    "augusti": "08",
    "september": "09",
    "oktober": "10",
    "november": "11",
    "december": "12",
}


def normalize_html_fragment_to_text(fragment: str) -> str:
    no_script = re.sub(r"<script\b[^>]*>.*?</script>", " ", fragment, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", no_script)
    text = html_lib.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def month_year_to_iso_month(month_name: str, year_text: str) -> Optional[str]:
    month_num = SWEDISH_MONTHS.get(month_name.lower())
    if not month_num:
        return None
    return f"{int(year_text):04d}-{month_num}"


def extract_period_arvsfonden_from_tidstatus(
    html: str,
) -> Tuple[Optional[str], Optional[str]]:
    open_date: Optional[str] = None
    close_date: Optional[str] = None
    alpha = r"[a-zA-ZåäöÅÄÖ]+"

    start_re = re.compile(
        rf"Projektet\s+startade\s+i\s+({alpha})\s+(\d{{4}})", re.IGNORECASE
    )
    granted_re = re.compile(
        rf"Projektet\s+beviljades\s+stöd\s+i\s+({alpha})\s+(\d{{4}})", re.IGNORECASE
    )
    close_re = re.compile(
        rf"avslut(?:ades|as)\s+i\s+({alpha})\s+(\d{{4}})", re.IGNORECASE
    )

    for section in iter_arvsfonden_tidstatus_sections(html):
        text = normalize_html_fragment_to_text(section)

        if open_date is None:
            start_match = start_re.search(text)
            if start_match:
                open_date = month_year_to_iso_month(
                    start_match.group(1), start_match.group(2)
                )

        if open_date is None:
            granted_match = granted_re.search(text)
            if granted_match:
                open_date = month_year_to_iso_month(
                    granted_match.group(1), granted_match.group(2)
                )

        if close_date is None:
            close_match = close_re.search(text)
            if close_match:
                close_date = month_year_to_iso_month(
                    close_match.group(1), close_match.group(2)
                )

        if open_date is not None and close_date is not None:
            break

    return open_date, close_date


def extract_arvsfonden_project_data(html: str) -> ExtractedProjectData:
    funding_year = extract_year_arvsfonden_from_description(html)
    open_date, close_date = extract_period_arvsfonden_from_tidstatus(html)
    return ExtractedProjectData(
        funding_year=funding_year,
        open_date=open_date,
        close_date=close_date,
    )


def extract_year_from_text(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    matches = re.findall(r"(19\d{2}|20\d{2})", value)
    if not matches:
        return None
    return int(matches[-1])


def normalize_optional_text(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def extract_eu_project_identifier(url: str) -> Optional[str]:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)

    for key in ("rcn", "id", "projectId", "project"):
        values = query.get(key, [])
        for value in values:
            match = re.search(r"\d{5,}", value)
            if match:
                return match.group(0)

    segments = [segment for segment in parsed.path.split("/") if segment]
    for index, segment in enumerate(segments):
        if segment.lower() == "id" and index + 1 < len(segments):
            match = re.search(r"\d{5,}", segments[index + 1])
            if match:
                return match.group(0)

    for segment in reversed(segments):
        match = re.search(r"\d{5,}", segment)
        if match:
            return match.group(0)

    fallback_match = re.search(r"\d{5,}", url)
    if fallback_match:
        return fallback_match.group(0)
    return None


def fetch_cordis_project_details(url: str, timeout: float) -> str:
    identifier = extract_eu_project_identifier(url)
    if not identifier:
        raise ValueError(f"Cannot detect CORDIS project identifier from URL: {url}")

    last_payload = ""
    for param_type in ("id", "rcn"):
        api_url = (
            "https://cordis.europa.eu/api/details"
            f"?contenttype=project&rcn={urllib.parse.quote(identifier)}"
            f"&lang=en&paramType={param_type}"
        )
        req = urllib.request.Request(
            api_url,
            headers={
                "User-Agent": "Mozilla/5.0 (funding-year-filler/1.0)",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            payload = response.read().decode(charset, errors="replace")
            last_payload = payload

        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and parsed.get("status") is True:
            return payload

    return last_payload


def extract_european_union_project_data(payload_text: str) -> ExtractedProjectData:
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return ExtractedProjectData()

    if not isinstance(payload, dict) or payload.get("status") is not True:
        return ExtractedProjectData()

    details = payload.get("payload")
    if not isinstance(details, dict):
        return ExtractedProjectData()

    information = details.get("information")
    if not isinstance(information, dict):
        return ExtractedProjectData()

    funding_year = extract_year_from_text(
        normalize_optional_text(information.get("ecSignatureDate"))
    )
    open_date = normalize_optional_text(information.get("startDateCode"))
    close_date = normalize_optional_text(information.get("endDateCode"))

    return ExtractedProjectData(
        funding_year=funding_year,
        open_date=open_date,
        close_date=close_date,
    )


SOURCES: Dict[str, SourceConfig] = {
    "arvsfonden": SourceConfig(
        name="arvsfonden",
        financier_value="Arvsfonden",
        fetcher=fetch_html,
        extractor=extract_arvsfonden_project_data,
    ),
    "european_union": SourceConfig(
        name="european_union",
        financier_value="European Union",
        fetcher=fetch_cordis_project_details,
        extractor=extract_european_union_project_data,
    ),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def configure_logger(level: str, log_file: Optional[str]) -> logging.Logger:
    logger = logging.getLogger("funding_year_filler")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(getattr(logging, level.upper()))
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def resolve_checkpoint_path(
    output_path: Path, checkpoint_file_arg: Optional[str]
) -> Path:
    if checkpoint_file_arg:
        return Path(checkpoint_file_arg)
    return output_path.with_suffix(f"{output_path.suffix}.checkpoint.json")


def load_checkpoint(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid checkpoint format in {path}")
    return data


def save_checkpoint(path: Path, data: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2, sort_keys=True)
        f.write("\n")


def build_checkpoint(
    input_path: Path,
    output_path: Path,
    source: str,
    financier_value: str,
    url_column: str,
    financier_column: str,
    funding_year_column: str,
    next_data_row_pos: int,
    stats: RunStats,
    completed: bool,
) -> Dict[str, object]:
    return {
        "version": CHECKPOINT_VERSION,
        "updated_at": utc_now_iso(),
        "completed": completed,
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
        "source": source,
        "financier_value": financier_value,
        "url_column": url_column,
        "financier_column": financier_column,
        "funding_year_column": funding_year_column,
        "next_data_row_pos": next_data_row_pos,
        "stats": stats.to_dict(),
    }


def validate_checkpoint(
    checkpoint: Dict[str, object],
    input_path: Path,
    output_path: Path,
    source: str,
    financier_value: str,
    url_column: str,
    financier_column: str,
    funding_year_column: str,
) -> Tuple[int, RunStats]:
    version = int(checkpoint.get("version", -1))
    if version != CHECKPOINT_VERSION:
        raise ValueError(
            f"Unsupported checkpoint version: {version} (expected {CHECKPOINT_VERSION})"
        )
    if bool(checkpoint.get("completed", False)):
        raise ValueError("Checkpoint already marked as completed.")
    if checkpoint.get("input_path") != str(input_path.resolve()):
        raise ValueError("Checkpoint input_path does not match current --input.")
    if checkpoint.get("output_path") != str(output_path.resolve()):
        raise ValueError("Checkpoint output_path does not match current --output.")
    if checkpoint.get("source") != source:
        raise ValueError("Checkpoint source does not match current --source.")
    if checkpoint.get("financier_value") != financier_value:
        raise ValueError("Checkpoint financier filter does not match current settings.")
    if checkpoint.get("url_column") != url_column:
        raise ValueError("Checkpoint url column does not match current settings.")
    if checkpoint.get("financier_column") != financier_column:
        raise ValueError("Checkpoint financier column does not match current settings.")
    if checkpoint.get("funding_year_column") != funding_year_column:
        raise ValueError("Checkpoint funding year column does not match current settings.")

    next_data_row_pos = int(checkpoint.get("next_data_row_pos", 0))
    raw_stats = checkpoint.get("stats", {})
    if not isinstance(raw_stats, dict):
        raise ValueError("Checkpoint stats format is invalid.")
    return next_data_row_pos, RunStats.from_dict(raw_stats)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fill Funding_year in XLSX by scraping project URLs. "
            "Funding year is extracted with source-specific logic."
        )
    )
    parser.add_argument(
        "--input", "-i", default="projects.xlsx", help="Input XLSX file (default: projects.xlsx)."
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output XLSX file. Default: <input>.with_funding_year.xlsx",
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=sorted(SOURCES.keys()),
        help="Source parser to use.",
    )
    parser.add_argument(
        "--financier-column",
        default="Financier",
        help="Column name for financier (default: Financier).",
    )
    parser.add_argument(
        "--url-column",
        default="url",
        help="Column name for project URL (default: url).",
    )
    parser.add_argument(
        "--funding-year-column",
        default="Funding_year",
        help="Column name to write year (default: Funding_year).",
    )
    parser.add_argument(
        "--financier-value",
        help="Exact financier value to match. Default comes from selected --source.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout in seconds (default: 20).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel fetch workers (default: 1, recommend 2-3).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only process up to N matching rows (for test runs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write output file, only print summary.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress per row.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint and existing output file.",
    )
    parser.add_argument(
        "--checkpoint-file",
        help=(
            "Checkpoint JSON path. "
            "Default: <output>.checkpoint.json"
        ),
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        help="Save checkpoint every N scanned rows (default: 50, 0 to disable).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Log summary progress every N scanned rows (default: 25, 0 to disable).",
    )
    parser.add_argument(
        "--log-file",
        help="Optional log file path.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Log level (default: INFO).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = configure_logger(args.log_level, args.log_file)

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        return 1

    output_path = Path(args.output) if args.output else input_path.with_name(
        f"{input_path.stem}.with_funding_year{input_path.suffix}"
    )
    checkpoint_path = resolve_checkpoint_path(output_path, args.checkpoint_file)

    source = SOURCES[args.source]
    financier_value = args.financier_value or source.financier_value

    if args.checkpoint_every < 0:
        logger.error("--checkpoint-every must be >= 0")
        return 1
    if args.progress_every < 0:
        logger.error("--progress-every must be >= 0")
        return 1
    if args.workers < 1:
        logger.error("--workers must be >= 1")
        return 1
    if args.limit is not None and args.limit < 0:
        logger.error("--limit must be >= 0")
        return 1
    if args.dry_run and args.resume:
        logger.warning("--dry-run with --resume will not persist new progress.")

    workbook_to_open = input_path
    start_data_row_pos = 0
    stats = RunStats()

    logger.info("Source parser: %s", source.name)
    logger.info("Financier filter: %s", financier_value)
    logger.info("Input workbook: %s", input_path)
    logger.info("Output workbook: %s", output_path)
    logger.info("Checkpoint file: %s", checkpoint_path)
    logger.info("Workers: %d", args.workers)

    if args.resume:
        if not checkpoint_path.exists():
            logger.error("Checkpoint not found for resume: %s", checkpoint_path)
            return 1
        try:
            checkpoint_data = load_checkpoint(checkpoint_path)
        except Exception as exc:
            logger.error("Failed to read checkpoint: %s", exc)
            return 1

        if bool(checkpoint_data.get("completed", False)):
            logger.info("Checkpoint already completed. Nothing to resume.")
            checkpoint_stats = checkpoint_data.get("stats", {})
            if isinstance(checkpoint_stats, dict):
                logger.info(
                    "Last summary: scanned=%s, financier_match=%s, fetched=%s, updated=%s, errors=%s",
                    checkpoint_stats.get("scanned", 0),
                    checkpoint_stats.get("matched_financier", 0),
                    checkpoint_stats.get("fetched", 0),
                    checkpoint_stats.get("updated", 0),
                    checkpoint_stats.get("errors", 0),
                )
            return 0

        try:
            start_data_row_pos, stats = validate_checkpoint(
                checkpoint=checkpoint_data,
                input_path=input_path,
                output_path=output_path,
                source=source.name,
                financier_value=financier_value,
                url_column=args.url_column,
                financier_column=args.financier_column,
                funding_year_column=args.funding_year_column,
            )
        except ValueError as exc:
            logger.error("Checkpoint validation failed: %s", exc)
            return 1

        if args.dry_run:
            workbook_to_open = input_path
        else:
            if not output_path.exists():
                logger.error(
                    "Cannot resume: output workbook not found (%s).", output_path
                )
                return 1
            workbook_to_open = output_path

        logger.info(
            "Resuming from data row position %d with stats=%s",
            start_data_row_pos,
            stats.to_dict(),
        )
    else:
        if checkpoint_path.exists():
            logger.warning(
                "Checkpoint file exists and may be overwritten when saving progress: %s",
                checkpoint_path,
            )

    files = load_xlsx_as_map(workbook_to_open)
    sheet_path = get_first_sheet_path(files)
    shared_strings = parse_shared_strings(files)
    sheet_root = ET.fromstring(files[sheet_path])
    sheet_data = sheet_root.find(f"{TAG}sheetData")
    if sheet_data is None:
        logger.error("Invalid worksheet: missing sheetData")
        return 1

    rows = sheet_data.findall(f"{TAG}row")
    if not rows:
        logger.error("Worksheet has no rows.")
        return 1

    header_values = read_row_values(rows[0], shared_strings)
    header_map: Dict[str, int] = {}
    for col_idx, (value, _) in header_values.items():
        header_map[value.strip().lower()] = col_idx

    missing = [
        original_name
        for original_name in (
            args.url_column,
            args.financier_column,
            args.funding_year_column,
        )
        if original_name.lower() not in header_map
    ]
    if missing:
        logger.error("Missing required columns: %s", ", ".join(missing))
        return 1

    url_col = header_map[args.url_column.lower()]
    financier_col = header_map[args.financier_column.lower()]
    funding_col = header_map[args.funding_year_column.lower()]
    open_col = funding_col + 1
    close_col = funding_col + 2

    header_row = rows[0]
    header_row_idx_text = header_row.get("r") or "1"
    header_row_idx = int(header_row_idx_text)
    header_values = read_row_values(header_row, shared_strings)
    open_header_value = header_values.get(open_col, ("", None))[0].strip()
    close_header_value = header_values.get(close_col, ("", None))[0].strip()
    if open_header_value == "":
        set_text_cell(header_row, open_col, header_row_idx, "Open_date")
    if close_header_value == "":
        set_text_cell(header_row, close_col, header_row_idx, "Close_date")

    data_rows = rows[1:]
    total_data_rows = len(data_rows)
    if start_data_row_pos > total_data_rows:
        logger.error(
            "Checkpoint next_data_row_pos (%d) exceeds data rows (%d).",
            start_data_row_pos,
            total_data_rows,
        )
        return 1

    logger.info(
        "Data rows=%d, start_data_row_pos=%d",
        total_data_rows,
        start_data_row_pos,
    )

    def persist_progress(next_data_row_pos: int, completed: bool, reason: str) -> None:
        if args.dry_run:
            return

        files[sheet_path] = ET.tostring(
            sheet_root, encoding="utf-8", xml_declaration=True
        )
        save_xlsx_from_map(output_path, files)

        checkpoint_payload = build_checkpoint(
            input_path=input_path,
            output_path=output_path,
            source=source.name,
            financier_value=financier_value,
            url_column=args.url_column,
            financier_column=args.financier_column,
            funding_year_column=args.funding_year_column,
            next_data_row_pos=next_data_row_pos,
            stats=stats,
            completed=completed,
        )
        save_checkpoint(checkpoint_path, checkpoint_payload)
        logger.info(
            "Saved progress (%s): next_data_row_pos=%d, scanned=%d, financier_match=%d, fetched=%d, updated=%d, errors=%d",
            reason,
            next_data_row_pos,
            stats.scanned,
            stats.matched_financier,
            stats.fetched,
            stats.updated,
            stats.errors,
        )

    next_data_row_pos = start_data_row_pos
    scanned_since_checkpoint = 0
    limit_reached = False
    completed_positions: set[int] = set()
    pending_fetches: List[PendingFetch] = []

    def advance_checkpoint_cursor() -> None:
        nonlocal next_data_row_pos
        while next_data_row_pos in completed_positions:
            completed_positions.remove(next_data_row_pos)
            next_data_row_pos += 1

    def mark_row_completed(data_row_pos: int) -> None:
        completed_positions.add(data_row_pos)
        advance_checkpoint_cursor()

    def resolve_fetch_result(pending: PendingFetch) -> None:
        try:
            result = pending.future.result()
            row_updated = False

            if result.funding_year is not None:
                set_numeric_cell(
                    pending.row, funding_col, pending.row_idx, result.funding_year
                )
                row_updated = True

            if result.open_date is not None:
                set_text_cell(pending.row, open_col, pending.row_idx, result.open_date)
                row_updated = True

            if result.close_date is not None:
                set_text_cell(
                    pending.row, close_col, pending.row_idx, result.close_date
                )
                row_updated = True

            if row_updated:
                stats.updated += 1
                if args.verbose:
                    logger.info(
                        "Row %d: funding_year=%s, open_date=%s, close_date=%s",
                        pending.row_idx,
                        result.funding_year,
                        result.open_date,
                        result.close_date,
                    )
            elif args.verbose:
                logger.info(
                    "Row %d: no funding year/date found from source sections",
                    pending.row_idx,
                )
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
        ) as exc:
            stats.errors += 1
            logger.warning("Row %d: fetch failed (%s)", pending.row_idx, exc)
        except Exception as exc:
            stats.errors += 1
            logger.error("Row %d: unexpected error (%s)", pending.row_idx, exc)
        finally:
            mark_row_completed(pending.data_row_pos)

    def drain_one_pending(block: bool) -> bool:
        if not pending_fetches:
            return False
        pending = pending_fetches[0]
        if not block and not pending.future.done():
            return False
        pending_fetches.pop(0)
        resolve_fetch_result(pending)
        return True

    def drain_all_pending(reason: str) -> None:
        if pending_fetches:
            logger.debug(
                "Draining %d pending fetch tasks (%s)", len(pending_fetches), reason
            )
        while pending_fetches:
            drain_one_pending(block=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        for data_row_pos in range(start_data_row_pos, total_data_rows):
            if args.limit is not None and stats.fetched >= args.limit:
                limit_reached = True
                logger.info(
                    "Reached fetch limit (--limit=%d) at data_row_pos=%d",
                    args.limit,
                    data_row_pos,
                )
                break

            row = data_rows[data_row_pos]
            scanned_since_checkpoint += 1
            stats.scanned += 1

            row_idx_text = row.get("r")
            if not row_idx_text:
                if args.verbose:
                    logger.info(
                        "Data row pos %d: skipped (missing row index)", data_row_pos
                    )
                mark_row_completed(data_row_pos)
            else:
                row_idx = int(row_idx_text)
                row_values = read_row_values(row, shared_strings)
                financier = row_values.get(financier_col, ("", None))[0].strip()
                if financier != financier_value:
                    if args.verbose:
                        logger.info(
                            "Row %d: skipped (financier '%s' != '%s')",
                            row_idx,
                            financier,
                            financier_value,
                        )
                    mark_row_completed(data_row_pos)
                else:
                    stats.matched_financier += 1
                    url = row_values.get(url_col, ("", None))[0].strip()
                    if not url:
                        if args.verbose:
                            logger.info("Row %d: skipped (empty URL)", row_idx)
                        mark_row_completed(data_row_pos)
                    else:
                        stats.fetched += 1
                        future = executor.submit(
                            fetch_project_data_from_url,
                            url,
                            args.timeout,
                            source,
                        )
                        pending_fetches.append(
                            PendingFetch(
                                data_row_pos=data_row_pos,
                                row=row,
                                row_idx=row_idx,
                                url=url,
                                future=future,
                            )
                        )

            # Keep at most N in-flight fetches.
            while len(pending_fetches) >= args.workers:
                drain_one_pending(block=True)

            # Opportunistically apply completed tasks.
            while drain_one_pending(block=False):
                continue

            if args.progress_every > 0 and stats.scanned % args.progress_every == 0:
                logger.info(
                    "Progress: scanned=%d/%d, financier_match=%d, fetched=%d, updated=%d, errors=%d, next_data_row_pos=%d",
                    stats.scanned,
                    total_data_rows,
                    stats.matched_financier,
                    stats.fetched,
                    stats.updated,
                    stats.errors,
                    next_data_row_pos,
                )

            if (
                args.checkpoint_every > 0
                and scanned_since_checkpoint >= args.checkpoint_every
            ):
                drain_all_pending(reason="periodic checkpoint")
                persist_progress(next_data_row_pos, completed=False, reason="periodic")
                scanned_since_checkpoint = 0

        drain_all_pending(reason="finalize")

    completed = next_data_row_pos >= total_data_rows and not limit_reached
    summary_line = (
        "Summary: "
        f"scanned={stats.scanned}, "
        f"financier_match={stats.matched_financier}, "
        f"fetched={stats.fetched}, "
        f"updated={stats.updated}, "
        f"errors={stats.errors}, "
        f"next_data_row_pos={next_data_row_pos}, "
        f"completed={completed}"
    )
    logger.info(summary_line)

    if args.dry_run:
        logger.info("Dry run enabled, no file/checkpoint written.")
        return 0

    persist_progress(next_data_row_pos, completed=completed, reason="final")
    logger.info("Wrote workbook: %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
