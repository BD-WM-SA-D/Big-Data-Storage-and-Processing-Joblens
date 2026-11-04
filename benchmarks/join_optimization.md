# Tối ưu nhiều join – kết quả

Số liệu do `spark-jobs/bench_joins.py` in ra log và ghi `s3://joblens/reports/join-bench/<thời điểm>.json` (cách làm:
[`../docs/thi-nghiem-spark.md`](../docs/thi-nghiem-spark.md) mục E). Chép nguyên văn; chưa chạy thì để `<cần đo>`.

## Lần 1 – `<ngày giờ>`

- Commit `<commit>`, Spark local mode (`EXECUTORS=0`, driver 2 GB), `--repeat 3`.
- Kích thước: silver `<cần đo>`, snapshot `<cần đo>`, bronze `<cần đo>` dòng.

`<cần đo – chép bảng "Kết quả" từ log>`

Mọi cấu hình cho cùng kết quả: `<cần đo>`.
