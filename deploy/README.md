# Triển khai nền tảng JobLens trên k3d

Thứ tự cài cho môi trường dev (một máy, cụm k3d `joblens` 1 server + 2 agent). Mọi lệnh chạy từ gốc repo, trong WSL/Linux. Mỗi script chạy lại nhiều lần vẫn an toàn.

Dựng tất cả một lần (máy mới, GitHub Codespaces – [`docs/cloud-codespaces.md`](../docs/cloud-codespaces.md)): `bash scripts/bootstrap-all.sh`.

| Bước | Lệnh | Namespace | Cài gì |
|---|---|---|---|
| −1 | `bash deploy/k3d/create-cluster.sh` (máy 8 GB: `JOBLENS_PROFILE=lite …`) | – | Cụm k3d `joblens` (chỉ khi chưa có cụm) |
| 0 | `cp .env.example .env && uv sync` | – | Biến môi trường, Python + pyspark (để chạy `spark-submit`) |
| 1 | `bash deploy/kafka/install.sh` | `ingest` | Strimzi 1.2.0, Kafka 4.3.1 KRaft 1 node, 7 topic (`deploy/kafka/topics.yaml`) |
| 2 | `bash deploy/storage/install.sh` | `lakehouse` | SeaweedFS 4.48 (S3, bucket `joblens`), Lakekeeper v0.13.3 + Postgres 17.6, warehouse `joblens` |
| 2b | `bash deploy/mongodb/install.sh` | `serving` | MongoDB 8.0 (NoSQL serving): user `joblens`, Secret `joblens-mongo` cho Spark và Trino |
| 3 | `bash deploy/trino/install.sh` | `serving` | Trino 483 (một node: coordinator kiêm worker – vừa máy 16 GB), catalog `iceberg` |
| 4 | `bash deploy/smoke-e2e.sh` | `compute` | Fixtures → Kafka → Spark streaming → `bronze.jobs_raw` → Trino |
| 5 | `bash deploy/data/upload-vietjobs.sh` | `lakehouse` | Tải VietJobs (Hugging Face) → `s3://joblens/raw/vietjobs/` |
| (demo) | `bash deploy/data/replay-vietjobs.sh 3000 50` | `compute` | Pod producer Python gửi tin VietJobs vào `jobs.raw.vietjobs.v1` cho job streaming |
| 6 | `DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze` | `compute` | 48k tin VietJobs vào `bronze.jobs_raw` (đường A – batch) |
| 7 | `bash deploy/spark/submit.sh silver_job_postings` | `compute` | `bronze.jobs_raw` → `silver.job_postings` (MERGE) + `silver.job_postings_snapshot` |
| 8 | `bash scripts/dbt.sh build` | `serving` (Trino) | Seed 34 tỉnh, `gold.dim_location`, `gold.mart_salary_role_region` + 12 test dữ liệu |
| 9 | `bash deploy/monitoring/install.sh` | `monitoring` | Grafana 13.2.4 + plugin Trino 1.4.1 + dashboard `dashboards/joblens-demo.json`; Kafka UI v1.5.0 (chỉ đọc) |
| 10 | `bash deploy/airflow/install.sh` | `orchestration` | Postgres 17.6 + Airflow 3.3.2 (chart 1.22.0, LocalExecutor), DAG `joblens_pipeline`: silver → GX → dbt gold + bảo trì Iceberg; build image `joblens/dbt` |
| 11 | `bash deploy/data-tools/install.sh` | `serving` | SQLPad 7.5.7 (SQL qua Trino trên trình duyệt, 8 câu mẫu) + mongo-express 1.0.2 (sửa document MongoDB) – tùy chọn, cho demo sửa dữ liệu không cần terminal |

Kịch bản demo môn học: [`docs/demo.md`](../docs/demo.md). Mở mọi giao diện web: `bash scripts/demo-ui.sh`. Truy vấn nhanh: `bash scripts/trino.sh "SELECT …"`.

Sau bước 4 thành công, thêm một dòng vào [`compat.md`](compat.md) (quy tắc của repo).

## Địa chỉ dịch vụ trong cụm

| Dịch vụ | Địa chỉ |
|---|---|
| Kafka bootstrap | `joblens-kafka-bootstrap.ingest.svc.cluster.local:9092` |
| S3 (SeaweedFS) | `http://seaweedfs-all-in-one.lakehouse.svc.cluster.local:8333`, bucket `joblens` |
| Iceberg REST catalog (Lakekeeper) | `http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog`, warehouse `joblens` |
| Trino | `http://trino.serving.svc.cluster.local:8080`, catalog `iceberg` (+ `mongodb` khi đã cài MongoDB) |
| MongoDB | `mongodb.serving.svc.cluster.local:27017`, database `joblens` (URL trong Secret `joblens-mongo`) |
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
- `spark-jobs/gx_silver.py` (task `gx_validate_silver`): Great Expectations 1.23 trên Spark – khóa, danh mục, khoảng lương, 34 mã tỉnh, độ phủ địa điểm ≥ 90%, lương ≥ 50%. Hỏng thì dbt không chạy (gold giữ bản cũ). Kết quả JSON ở `s3://joblens/quality/gx/silver_job_postings/`.
- MongoDB là tầng phục vụ NoSQL: `stream_province_windows.py` (streaming có state, watermark) upsert `province_windows`; `analytics_spark.py` (task `spark_analytics`) ghi `analytics_*`. Collection nội bộ của repo này nên chưa có contract ở `joblens`; repo khác cần đọc thì thêm contract trước.
- `spark-jobs/maintenance_iceberg.py` (task `iceberg_maintenance`): `rewrite_data_files` gộp file nhỏ do streaming, `rewrite_manifests`, `expire_snapshots` (giữ 10 snapshot gần nhất). Không chạy `remove_orphan_files` khi streaming đang ghi.
- Checkpoint của job streaming nằm trên PVC `compute/spark-checkpoints`. Xóa PVC thì job đọc lại Kafka từ đầu (bronze sẽ có bản ghi lặp).

## Gỡ

```bash
k3d cluster delete joblens && k3d cluster create joblens --servers 1 --agents 2
```

## Chọn cấu hình theo RAM máy

Đặt một lần trong `~/.bashrc` rồi mở terminal mới: `echo 'export JOBLENS_PROFILE=lite' >> ~/.bashrc`.
Mọi script trong `deploy/` và `scripts/` đọc biến này (không ghi vào `.env` vì `.env` đồng bộ từ repo `joblens`).

| RAM máy | Profile | WSL (`C:\Users\<tên>\.wslconfig`) | Khác gì |
|---|---|---|---|
| ≥ 16 GB | `full` (mặc định) | `memory=12GB`, `swap=8GB` | Cụm 1 server + 2 agent; Spark cluster mode có pod executor riêng; Kafka UI |
| 8 GB | `lite` | `memory=5GB`, `swap=6GB` | Cụm 1 node, tắt Traefik; Spark **local mode** (`EXECUTORS=0`, cả job trong một pod, kể cả task Airflow); Kafka heap 256 MB; Trino heap 768 MB; Airflow nhỏ hơn; không cài Kafka UI |
| 4 GB | – | – | Không đủ để chạy cụm (xem dưới) |

**Ước tính (chưa đo trên máy 8 GB – ai chạy `lite` hãy ghi số thật vào `deploy/compat.md`):** dựa trên số đo cấu hình
`full` ngày 2026-10-20 (`kubectl top`: cả cụm ~5,3 GB khi nhàn rỗi, trong đó Trino 771 MiB, Kafka 538 MiB), `lite`
khi nhàn rỗi khoảng 3–3,5 GB; lúc chạy một job Spark local mode thêm ~1,2–1,5 GB. Vì vậy với `lite`:

- Chạy **một** job Spark mỗi lúc: dừng streaming trước khi trigger DAG (`bash scripts/demo-mode.sh airflow`), DAG đã
  xếp các task Spark tuần tự.
- Chỉ cài Airflow/Grafana khi cần trình diễn; tắt khi không dùng:
  `kubectl scale -n orchestration statefulset/airflow-scheduler deploy/airflow-dag-processor deploy/airflow-api-server --replicas=0`.
- Đóng bớt ứng dụng Windows; trình duyệt chỉ mở tab cần thiết.

Chuyển cụm `full` đang có sang `lite` (giữ dữ liệu trong cụm): đặt `JOBLENS_PROFILE=lite` rồi chạy lại
`deploy/kafka/install.sh`, `deploy/trino/install.sh`, `deploy/airflow/install.sh`, `deploy/monitoring/install.sh`.
Muốn cụm 1 node thì phải tạo lại cụm (`k3d cluster delete joblens` – **mất dữ liệu trong cụm**) rồi làm lại các bước.

**Máy 4 GB:** Windows đã dùng ~2–2,5 GB, phần còn lại không đủ cho Kubernetes + Kafka (JVM) + Spark (JVM) + Trino
(JVM) cùng lúc – cắt bớt nữa thì không còn là nền tảng của môn. Cách làm việc được:

- Viết và kiểm thử mã không cần cụm: `uv sync && uv run pytest -q` (cần Java 17 cho test Spark), `dbt parse`.
- Chạy cụm trên máy của bạn trong nhóm có RAM ≥ 16 GB, hoặc một VM cloud (credit sinh viên), rồi port-forward giao diện.

Xem RAM bất cứ lúc nào: `bash scripts/demo-mode.sh status`. Không chạy job streaming và DAG Airflow cùng lúc:
`bash scripts/demo-mode.sh streaming|airflow`.
