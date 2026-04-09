# Fill Funding Year

Script: [`fill_funding_year.py`](/home/marcus/getFundingYear/fill_funding_year.py)

## Mục tiêu

Điền cột `Funding_year` trong file Excel dựa trên dữ liệu từ URL project.
Đồng thời điền thêm thông tin theo source:
- `Open_date`, `Close_date` theo project period
- với `european_union`: thêm `sweden`, `full_funding`

Hiện tại hỗ trợ:
- `arvsfonden`
- `european_union`

## Logic hiện tại

### Arvsfonden
Với mỗi dòng có `Financier = Arvsfonden`:
1. Mở URL ở cột `url`.
2. Chỉ tìm trong phần description của project (khối có `Beskrivning av projektet` trong main content left column).
3. Tìm pattern diary number:
   - `Diarienummer: ...-YYYY`
4. Lấy `YYYY` làm `Funding_year`.
5. Tìm phần `Projektets tidstatus` để lấy period:
   - Ví dụ `Projektet startade i januari 2024 och avslutades i december 2024.`
   - Ví dụ `Projektet beviljades stöd i december 2023.`
6. Ghi:
   - `Open_date`: lấy từ `startade i ...` hoặc fallback trong cùng section là `beviljades stöd i ...`
   - `Close_date`: lấy từ `avslutades i ...` / `avslutas i ...`

### European Union
Với mỗi dòng có `Financier = European Union`:
1. Lấy project identifier từ URL (ví dụ `https://cordis.europa.eu/project/id/101000395` -> `101000395`).
2. Gọi API CORDIS:
   - `https://cordis.europa.eu/api/details?contenttype=project&rcn=<identifier>&lang=en&paramType=id`
3. Parse JSON `payload.information`:
   - `Funding_year` = năm của `ecSignatureDate`
   - `Open_date` = `startDateCode`
   - `Close_date` = `endDateCode`
   - `full_funding` = `ecContribution`
4. Parse JSON `payload.organizations`:
   - `sweden` = `true` nếu có ít nhất 1 tổ chức có `country.name = Sweden`, ngược lại `false`

### Nguyên tắc quan trọng

- Không fallback sang nguồn khác/field khác.
- Nếu URL lỗi, timeout, 404 hoặc không lấy được field theo rule của source:
  - giữ nguyên ô tương ứng (không ghi gì mới).
- Nếu không có close date:
  - giữ trống `Close_date`.
- Date format theo source:
  - `arvsfonden`: `Open_date` / `Close_date` là `YYYY-MM`
  - `european_union`: `Open_date` / `Close_date` là `YYYY-MM-DD` (theo `startDateCode`/`endDateCode`)

## Yêu cầu file input

Sheet đầu tiên của workbook cần có các cột:
- `url`
- `Financier`
- `Funding_year`

Bạn có thể đổi tên cột qua CLI option (`--url-column`, `--financier-column`, `--funding-year-column`).

Các cột output được ghi ngay sau cột `Funding_year`:
- Luôn có: `Open_date`, `Close_date`
- Với source `european_union`: thêm `sweden`, `full_funding`
- Nếu header tương ứng đang trống, script tự đặt tên mặc định theo các tên ở trên.

## Cài đặt

Không cần package ngoài. Chạy bằng `python3`.

## Cách dùng nhanh

Chạy đầy đủ:

```bash
python3 fill_funding_year.py --source arvsfonden -i projects.xlsx -o projects.with_funding_year.xlsx --workers 3
```

Chạy cho European Union:

```bash
python3 fill_funding_year.py --source european_union -i projects.xlsx -o projects.with_funding_year.xlsx --workers 3
```

Chạy thử trước (không ghi file):

```bash
python3 fill_funding_year.py --source arvsfonden -i projects.xlsx --dry-run --limit 10 --verbose
```

## Logging

Mặc định script log ra stdout.

Option liên quan:
- `--log-level DEBUG|INFO|WARNING|ERROR` (default: `INFO`)
- `--log-file <path>` để ghi log ra file
- `--progress-every <N>` log summary mỗi N row scan (default: `25`, `0` = tắt)
- `--verbose` log chi tiết theo từng row

Ví dụ:

```bash
python3 fill_funding_year.py --source arvsfonden --workers 3 --log-file run.log --log-level INFO
```

## Chạy nhiều luồng (worker)

Script hỗ trợ xử lý song song URL qua option `--workers`.

- `--workers 1`: chạy tuần tự (mặc định)
- `--workers 2` hoặc `--workers 3`: chạy 2-3 luồng song song (khuyến nghị)

Ví dụ:

```bash
python3 fill_funding_year.py --source arvsfonden -i projects.xlsx -o projects.with_funding_year.xlsx --workers 2
```

## Checkpoint và Resume

Script hỗ trợ chạy tiếp khi bị dừng giữa chừng.

### Cách hoạt động

- Mỗi lần lưu progress, script ghi:
  - output workbook (đã cập nhật tới thời điểm đó)
  - checkpoint JSON (trạng thái `next_data_row_pos`, counters, config)
- Mặc định checkpoint path:
  - `<output>.checkpoint.json`

### Option liên quan

- `--checkpoint-file <path>`: chỉ định checkpoint file
- `--checkpoint-every <N>`: lưu checkpoint định kỳ mỗi N row scan (default `50`, `0` = tắt periodic save)
- `--resume`: chạy tiếp từ checkpoint

### Ví dụ resume

Lần 1 (chạy):

```bash
python3 fill_funding_year.py --source arvsfonden -i projects.xlsx -o projects.with_funding_year.xlsx
```

Lần 2 (nếu run trước bị dừng):

```bash
python3 fill_funding_year.py --source arvsfonden -i projects.xlsx -o projects.with_funding_year.xlsx --resume
```

## Danh sách options chính

```text
--source                 Bắt buộc. Source parser (arvsfonden, european_union)
--input, -i              File Excel input (default: projects.xlsx)
--output, -o             File Excel output
--financier-value        Override giá trị financier cần match
--timeout                Timeout request (giây)
--workers                Số worker fetch song song (default: 1, khuyến nghị 2-3)
--limit                  Chỉ xử lý tối đa N URL phù hợp (để test)
--dry-run                Không ghi output/checkpoint
--verbose                Log chi tiết theo row
--resume                 Resume từ checkpoint
--checkpoint-file        Đường dẫn checkpoint JSON
--checkpoint-every       Lưu checkpoint mỗi N row scan
--progress-every         Log summary mỗi N row scan
--log-file               Ghi log ra file
--log-level              Mức log
```
