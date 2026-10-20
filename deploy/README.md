# Triển khai nền tảng JobLens trên k3d

Thứ tự cài cho môi trường dev (một máy, cụm k3d `joblens` 1 server + 2 agent). Mọi lệnh chạy từ gốc repo, trong WSL/Linux. Mỗi script chạy lại nhiều lần vẫn an toàn.

| Bước | Lệnh | Namespace | Cài gì |
|---|---|---|---|
| 0 | `cp .env.example .env && uv sync` | – | Biến môi trường, Python + pyspark (để chạy `spark-submit`) |
| 1 | `bash deploy/kafka/install.sh` | `ingest` | Strimzi 1.2.0, Kafka 4.3.1 KRaft 1 node, 7 topic (`deploy/kafka/topics.yaml`) |
| 2 | `bash deploy/storage/install.sh` | `lakehouse` | SeaweedFS 4.48 (S3, bucket `joblens`), Lakekeeper v0.13.3 + Postgres 17.6, warehouse `joblens` |
| 3 | `bash deploy/trino/install.sh` | `serving` | Trino 483 (một node: coordinator kiêm worker – vừa máy 16 GB), catalog `iceberg` |
| 4 | `bash deploy/smoke-e2e.sh` | `compute` | Fixtures → Kafka → Spark streaming → `bronze.jobs_raw` → Trino |
| 5 | `bash deploy/data/upload-vietjobs.sh` | `lakehouse` | Tải VietJobs (Hugging Face) → `s3://joblens/raw/vietjobs/` |
| (demo) | `bash deploy/data/replay-vietjobs.sh 3000 50` | `compute` | Pod producer Python gửi tin VietJobs vào `jobs.raw.vietjobs.v1` cho job streaming |
| 6 | `DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze` | `compute` | 48k tin VietJobs vào `bronze.jobs_raw` (đường A – batch) |
| 7 | `bash deploy/spark/submit.sh silver_job_postings` | `compute` | `bronze.jobs_raw` → `silver.job_postings` (MERGE) + `silver.job_postings_snapshot` |
| 8 | `bash scripts/dbt.sh build` | `serving` (Trino) | Seed 34 tỉnh, `gold.dim_location`, `gold.mart_salary_role_region` + 12 test dữ liệu |
| 9 | `bash deploy/monitoring/install.sh` | `monitoring` | Grafana 13.2.4 + plugin Trino 1.4.1 + dashboard `dashboards/joblens-demo.json`; Kafka UI v1.5.0 (chỉ đọc) |
| 10 | `bash deploy/airflow/install.sh` | `orchestration` | Postgres 17.6 + Airflow 3.3.2 (chart 1.22.0, LocalExecutor), DAG `joblens_pipeline`: silver → dbt gold + bảo trì Iceberg; build image `joblens/dbt` |

Kịch bản demo môn học: [`docs/demo.md`](../docs/demo.md). Mở mọi giao diện web: `bash scripts/demo-ui.sh`. Truy vấn nhanh: `bash scripts/trino.sh "SELECT …"`.

Sau bước 4 thành công, thêm một dòng vào [`compat.md`](compat.md) (quy tắc của repo).

## Địa chỉ dịch vụ trong cụm

| Dịch vụ | Địa chỉ |
|---|---|
| Kafka bootstrap | `joblens-kafka-bootstrap.ingest.svc.cluster.local:9092` |
| S3 (SeaweedFS) | `http://seaweedfs-all-in-one.lakehouse.svc.cluster.local:8333`, bucket `joblens` |
| Iceberg REST catalog (Lakekeeper) | `http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog`, warehouse `joblens` |
| Trino | `http://trino.serving.svc.cluster.local:8080`, catalog `iceberg` |
| Airflow API/UI | `http://airflow-api-server.orchestration.svc.cluster.local:8080` |
| Grafana | `http://grafana.monitoring.svc.cluster.local:3000` |
| Kafka UI | `http://kafka-ui.monitoring.svc.cluster.local:8080` |

Từ máy mình: `bash scripts/demo-ui.sh` mở cùng lúc Grafana (3000), Kafka UI (8081), Airflow (8082), Trino UI (8080), Lakekeeper UI (8181, `/ui`), SeaweedFS filer (8888); `bash scripts/demo-ui.sh spark` mở Spark UI (4040) của driver mới nhất.

## Bí mật

- Khóa S3 nằm trong `.env` (đã gitignore). `deploy/storage/install.sh` sinh khóa nếu trống, rồi mỗi script tạo Secret `joblens-s3` trong namespace của nó.
- Mật khẩu Postgres và khóa mã hóa của Lakekeeper chỉ nằm trong Secret của cụm (`lakehouse/lakekeeper-pg`, `lakehouse/lakekeeper-encryption`).
- Môi trường dev **không bật xác thực** cho Lakekeeper và Trino – chỉ truy cập trong cụm hoặc qua port-forward.
- Airflow: Secret `orchestration/airflow-pg`, `airflow-metadata`, `airflow-keys` (fernet, API, JWT), `airflow-admin` (mật khẩu `admin`); Grafana: `monitoring/grafana-admin`. Tất cả do script sinh một lần, không nằm trong git. Kafka UI không đăng nhập, chế độ chỉ đọc.

## Ghi chú thiết kế

- Topic chỉ tạo bằng `KafkaTopic`; Kafka tắt `auto.create.topics.enable`. `jobs.raw.vietjobs.v1` dùng để replay VietJobs (demo, đo tải).
- Chuẩn hóa ở silver bằng quy tắc (`spark-jobs/normalize.py`, ADR 0001 D2); ánh xạ địa điểm dùng chung một file với dbt: `dbt/seeds/locations_2025.csv`.
- Job silver xử lý tăng dần: mốc `ingest_ts` đã xử lý lưu ở thuộc tính bảng `joblens.bronze_watermark`; `--full` để xử lý lại từ đầu.
- Warehouse tắt STS và remote signing: Spark và Trino dùng khóa S3 tĩnh từ Secret. Bật STS khi cần cấp quyền theo bảng.
- Spark chạy bằng `spark-submit --master k8s://` (cluster mode), image `joblens/spark-jobs:<version>-<gitsha>` build từ `spark-jobs/Dockerfile` và nạp vào k3d bằng `k3d image import` – không cần registry.
- Airflow chạy LocalExecutor; mỗi task của DAG là một pod trong `compute` (KubernetesPodOperator). Task Spark chạy client mode – pod task là driver, tạo pod executor bằng SA `spark` – nên log Spark hiện thẳng trên Airflow. DAG nạp từ ConfigMap `joblens-dags`; tag image là Airflow Variable `joblens_spark_image`, `joblens_dbt_image` khai báo qua Helm values (`deploy/airflow/install.sh`).
- `spark-jobs/maintenance_iceberg.py` (task `iceberg_maintenance`): `rewrite_data_files` gộp file nhỏ do streaming, `rewrite_manifests`, `expire_snapshots` (giữ 10 snapshot gần nhất). Không chạy `remove_orphan_files` khi streaming đang ghi.
- Checkpoint của job streaming nằm trên PVC `compute/spark-checkpoints`. Xóa PVC thì job đọc lại Kafka từ đầu (bronze sẽ có bản ghi lặp).

## Gỡ

```bash
k3d cluster delete joblens && k3d cluster create joblens --servers 1 --agents 2
```

## Máy dev 16 GB RAM

Cấu hình đã thu nhỏ: Trino một node (~1,8 GB), Kafka heap 512 MB và chỉ có topic operator, Airflow LocalExecutor
(~1,5 GB), Kafka UI heap 192 MB, replay Kafka bằng producer Python. Cho WSL `memory=12GB`, `swap=8GB` trong
`C:\Users\<tên>\.wslconfig`. Không chạy job streaming và DAG Airflow cùng lúc: `bash scripts/demo-mode.sh streaming|airflow`.
Xem RAM: `bash scripts/demo-mode.sh status`.
