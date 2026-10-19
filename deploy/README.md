# Triển khai nền tảng JobLens trên k3d

Thứ tự cài cho môi trường dev (một máy, cụm k3d `joblens` 1 server + 2 agent). Mọi lệnh chạy từ gốc repo, trong WSL/Linux. Mỗi script chạy lại nhiều lần vẫn an toàn.

| Bước | Lệnh | Namespace | Cài gì |
|---|---|---|---|
| 0 | `cp .env.example .env && uv sync` | – | Biến môi trường, Python + pyspark (để chạy `spark-submit`) |
| 1 | `bash deploy/kafka/install.sh` | `ingest` | Strimzi 1.2.0, Kafka 4.3.1 KRaft 1 node, 7 topic (`deploy/kafka/topics.yaml`) |
| 2 | `bash deploy/storage/install.sh` | `lakehouse` | SeaweedFS 4.48 (S3, bucket `joblens`), Lakekeeper v0.13.3 + Postgres 17.6, warehouse `joblens` |
| 3 | `bash deploy/trino/install.sh` | `serving` | Trino 483 (1 coordinator + 1 worker), catalog `iceberg` |
| 4 | `bash deploy/smoke-e2e.sh` | `compute` | Fixtures → Kafka → Spark streaming → `bronze.jobs_raw` → Trino |
| 5 | `bash deploy/data/upload-vietjobs.sh` | `lakehouse` | Tải VietJobs (Hugging Face) → `s3://joblens/raw/vietjobs/` |
| 6 | `DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze` | `compute` | 48k tin VietJobs vào `bronze.jobs_raw` (đường A – batch) |
| 7 | `bash deploy/spark/submit.sh silver_job_postings` | `compute` | `bronze.jobs_raw` → `silver.job_postings` (MERGE) + `silver.job_postings_snapshot` |
| 8 | `bash scripts/dbt.sh build` | `serving` (Trino) | Seed 34 tỉnh, `gold.dim_location`, `gold.mart_salary_role_region` + 12 test dữ liệu |

Kịch bản demo môn học: [`docs/demo.md`](../docs/demo.md). Truy vấn nhanh: `bash scripts/trino.sh "SELECT …"`.

Sau bước 4 thành công, thêm một dòng vào [`compat.md`](compat.md) (quy tắc của repo).

## Địa chỉ dịch vụ trong cụm

| Dịch vụ | Địa chỉ |
|---|---|
| Kafka bootstrap | `joblens-kafka-bootstrap.ingest.svc.cluster.local:9092` |
| S3 (SeaweedFS) | `http://seaweedfs-all-in-one.lakehouse.svc.cluster.local:8333`, bucket `joblens` |
| Iceberg REST catalog (Lakekeeper) | `http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog`, warehouse `joblens` |
| Trino | `http://trino.serving.svc.cluster.local:8080`, catalog `iceberg` |

Từ máy mình: `kubectl port-forward -n serving svc/trino 8080:8080` (Trino UI), `kubectl port-forward -n lakehouse svc/lakekeeper 8181:8181` (Lakekeeper UI ở `/ui`).

## Bí mật

- Khóa S3 nằm trong `.env` (đã gitignore). `deploy/storage/install.sh` sinh khóa nếu trống, rồi mỗi script tạo Secret `joblens-s3` trong namespace của nó.
- Mật khẩu Postgres và khóa mã hóa của Lakekeeper chỉ nằm trong Secret của cụm (`lakehouse/lakekeeper-pg`, `lakehouse/lakekeeper-encryption`).
- Môi trường dev **không bật xác thực** cho Lakekeeper và Trino – chỉ truy cập trong cụm hoặc qua port-forward.

## Ghi chú thiết kế

- Topic chỉ tạo bằng `KafkaTopic`; Kafka tắt `auto.create.topics.enable`. `jobs.raw.vietjobs.v1` dùng để replay VietJobs (demo, đo tải).
- Chuẩn hóa ở silver bằng quy tắc (`spark-jobs/normalize.py`, ADR 0001 D2); ánh xạ địa điểm dùng chung một file với dbt: `dbt/seeds/locations_2025.csv`.
- Job silver xử lý tăng dần: mốc `ingest_ts` đã xử lý lưu ở thuộc tính bảng `joblens.bronze_watermark`; `--full` để xử lý lại từ đầu.
- Warehouse tắt STS và remote signing: Spark và Trino dùng khóa S3 tĩnh từ Secret. Bật STS khi cần cấp quyền theo bảng.
- Spark chạy bằng `spark-submit --master k8s://` (cluster mode), image `joblens/spark-jobs:<version>-<gitsha>` build từ `spark-jobs/Dockerfile` và nạp vào k3d bằng `k3d image import` – không cần registry.
- Checkpoint của job streaming nằm trên PVC `compute/spark-checkpoints`. Xóa PVC thì job đọc lại Kafka từ đầu (bronze sẽ có bản ghi lặp).

## Gỡ

```bash
k3d cluster delete joblens && k3d cluster create joblens --servers 1 --agents 2
```
