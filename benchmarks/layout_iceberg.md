# Bố cục vật lý Iceberg: sắp xếp và partition evolution

Thiết kế: [`../docs/bien-doi-du-lieu.md`](../docs/bien-doi-du-lieu.md) mục 4. Số file ứng viên lấy từ log của
`spark-jobs/layout_iceberg.py` (và `s3://joblens/reports/layout/`); byte Trino đọc lấy từ `bash scripts/bench-layout.sh`
chạy trước và sau job. Chỉ ghi số đo thật; chưa đo thì để `<cần đo>`.

## Lần 1 – 2026-11-15 19:45 (giờ VN)

- Mã: `42fff10`; Airflow tắt (`scripts/demo-mode.sh streaming`). Kết quả JSON:
  `s3://joblens/reports/layout/20261115T124547Z.json`. Physical input đo bằng `scripts/bench-layout.sh` trước và sau job.

| Bảng | Truy vấn mẫu | Giai đoạn | Số file | MB | File ứng viên | MB ứng viên | Physical input (Trino) |
|---|---|---|---|---|---|---|---|
| bronze.jobs_raw (vietjobs) | `source_job_id = '16055'` | trước | 15 | 41.99 | 11 | 39.57 | 10.29 MB |
| bronze.jobs_raw (vietjobs) | `source_job_id = '16055'` | sau sort | 20 | 29.13 | 6 | 7.34 | 7.34 MB |
| silver.job_postings_history | 7 ngày gần nhất | trước | 8 | 2.9 | 8 | 2.9 | 2.90 MB |
| silver.job_postings_history | 7 ngày gần nhất | sau partition evolution | 16 | 2.98 | 8 | 0.19 | 191.85 kB |

Nhận xét:
- **Sort bronze:** file ứng viên 11 → 6, MB ứng viên 39,57 → 7,34 (−81%); byte Trino đọc 10,29 → 7,34 MB (−29%).
  Byte Trino đọc trước khi sort nhỏ hơn MB ứng viên vì Trino chỉ đọc cột cần (`source_job_id`, `crawl_ts`) và còn bỏ
  qua row group theo min/max bên trong Parquet. Số ứng viên không về 1 vì tin `16055` có 11 dòng (nạp nhiều lần) và
  sort chỉ xếp lại **trong từng partition** `days(ingest_ts), source` – mỗi ngày nạp có tin này vẫn còn một file chứa nó
  (kiểm tra: `SELECT count(DISTINCT date(ingest_ts)) FROM iceberg.bronze.jobs_raw WHERE source_job_id = '16055'`).
- Tổng dung lượng bronze giảm 41,99 → 29,13 MB (−31%) dù dữ liệu không đổi: các dòng giống nhau nằm cạnh nhau nên
  nén zstd tốt hơn. Số file tăng 15 → 20 vì kích thước file đích = kích thước trung bình cũ, tính trước khi nén tốt hơn.
- **Partition evolution history:** số file ứng viên vẫn 8 (một file mỗi bucket trong tháng hiện tại) nhưng các file đó
  chỉ còn dữ liệu của tháng này: MB ứng viên 2,9 → 0,19, byte Trino đọc 2,90 MB → 191,85 kB (−93%). Bảng không phải tạo
  lại, câu lệnh cũ và dữ liệu cũ vẫn đọc được.
- Giới hạn: `iceberg_maintenance` (binpack) có thể gộp lại file bronze đã sort; ở dữ liệu lớn nên đổi bảo trì bronze
  sang strategy `sort`.
