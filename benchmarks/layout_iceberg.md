# Bố cục vật lý Iceberg: sắp xếp và partition evolution

Thiết kế: [`../docs/bien-doi-du-lieu.md`](../docs/bien-doi-du-lieu.md) mục 4. Số file ứng viên lấy từ log của
`spark-jobs/layout_iceberg.py` (và `s3://joblens/reports/layout/`); byte Trino đọc lấy từ `bash scripts/bench-layout.sh`
chạy trước và sau job. Chỉ ghi số đo thật; chưa đo thì để `<cần đo>`.

## Lần 1 – `<ngày giờ>`

- Mã: `<commit>`; DAG tạm dừng (`scripts/demo-mode.sh streaming`).

| Bảng | Truy vấn mẫu | Giai đoạn | Số file | MB | File ứng viên | MB ứng viên | Physical input (Trino) |
|---|---|---|---|---|---|---|---|
| bronze.jobs_raw (vietjobs) | `source_job_id = '<id>'` | trước | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| bronze.jobs_raw (vietjobs) | `source_job_id = '<id>'` | sau sort | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| silver.job_postings_history | 7 ngày gần nhất | trước | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| silver.job_postings_history | 7 ngày gần nhất | sau partition evolution | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` | `<cần đo>` |

Nhận xét: `<cần đo>`
