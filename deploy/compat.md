# Bảng phiên bản các lần chạy

Mỗi buổi demo và mỗi lần chốt số liệu đều phải dựng lại được từ một dòng của bảng này (quy tắc 4 trong tài liệu kế hoạch, repo `joblens`). Không dùng `latest`. Image ghi tag dạng `version-gitsha`.

| Ngày | platform | mining (images) | app | contracts (`joblens`) | Iceberg tag | Ghi chú |
|---|---|---|---|---|---|---|
| <yyyy-mm-dd> | v<x.y.z> | <image:x.y.z-sha> | v<x.y.z> | v<x.y.z> | <…> | <…> |
| 2026-10-17 | 9432fed (walking skeleton) | – (chưa có image mining) | – | joblens@9750b1a | – | Smoke test fixtures → Kafka → Spark → bronze.jobs_raw → Trino đạt; Strimzi 1.2.0, SeaweedFS chart 4.48.0, Lakekeeper chart 0.12.0, Trino 483, spark-jobs:0.1.0-9432fed |
| 2026-10-19 | 455fc2f (silver + gold, VietJobs) | – (chưa có image mining) | – | joblens@9750b1a | silver.job_postings snapshot 7734621906540530803 | VietJobs 48.092 tin → bronze (batch) → silver (MERGE) → dbt gold PASS=15; có địa điểm 0,985, có lương 0,715; spark-jobs:0.1.0-455fc2f; Strimzi 1.2.0, SeaweedFS 4.48.0, Lakekeeper 0.12.0, Trino 483, Spark 4.1.3, Iceberg 1.11.0 – kiểm tra: hieu |
| 2026-10-23 | 6e86d04 (thí nghiệm batch vs streaming, lần 1) | – (chưa có image mining) | – | joblens@9750b1a | – | `scripts/benchmark-batch-vs-streaming.sh 5000 100 "10 seconds"`, profile full: độ đúng 0; trễ p95 batch 122,0 s, streaming 60,6 s; streaming 2 file, RAM không tải 1377 MiB; chi phí đường A chưa đo – số liệu đầy đủ ở benchmarks/batch_vs_streaming.md; spark-jobs:0.1.0-b67fd01 (A) và 0.1.0-6e86d04 (B) – mã trong spark-jobs/ giống nhau giữa hai commit; Airflow 3.3.2 (chart 1.22.0), GX 1.23.2 – kiểm tra: <người kiểm tra> |
| 2026-10-23 | 157f045 (MongoDB, spark_analytics, GX) | – (chưa có image mining) | – | joblens@9750b1a | – | DAG joblens_pipeline 5/5 task thành công (21:41–21:46); plan_checks broadcast/sort-merge/pivot/pushdown đều true; MLlib RMSE 10,32 so với đoán trung vị 11,20 triệu VND, R² log 0,370 – benchmarks/spark_analytics.md; MongoDB 8.0.32, Trino catalog mongodb; spark-jobs:0.1.0-157f045 – kiểm tra: <người kiểm tra> |
