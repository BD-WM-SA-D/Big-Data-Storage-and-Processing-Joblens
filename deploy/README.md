# Triển khai nền tảng JobLens trên k3d

Thứ tự cài cho môi trường dev (một máy, cụm k3d `joblens` 1 server + 2 agent). Mọi lệnh chạy từ gốc repo, trong WSL/Linux. Mỗi script chạy lại nhiều lần vẫn an toàn.

| Bước | Lệnh | Namespace | Cài gì |
|---|---|---|---|
| 0 | `cp .env.example .env && uv sync` | – | Biến môi trường, Python + pyspark (để chạy `spark-submit`) |
| 1 | `bash deploy/kafka/install.sh` | `ingest` | Strimzi 1.2.0, Kafka 4.3.1 KRaft 1 node, 7 topic (`deploy/kafka/topics.yaml`) |
| 2 | `bash deploy/storage/install.sh` | `lakehouse` | SeaweedFS 4.48 (S3, bucket `joblens`), Lakekeeper v0.13.3 + Postgres 17.6, warehouse `joblens` |
| 3 | `bash deploy/trino/install.sh` | `serving` | Trino 483 (1 coordinator + 1 worker), catalog `iceberg` |
| 4 | `bash deploy/smoke-e2e.sh` | `compute` | Fixtures → Kafka → Spark streaming → `bronze.jobs_raw` → Trino |

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

- Topic chỉ tạo bằng `KafkaTopic`; Kafka tắt `auto.create.topics.enable`.
- Warehouse tắt STS và remote signing: Spark và Trino dùng khóa S3 tĩnh từ Secret. Bật STS khi cần cấp quyền theo bảng.
- Spark chạy bằng `spark-submit --master k8s://` (cluster mode), image `joblens/spark-jobs:<version>-<gitsha>` build từ `spark-jobs/Dockerfile` và nạp vào k3d bằng `k3d image import` – không cần registry.
- Checkpoint của job streaming nằm trên PVC `compute/spark-checkpoints`. Xóa PVC thì job đọc lại Kafka từ đầu (bronze sẽ có bản ghi lặp).

## Gỡ

```bash
k3d cluster delete joblens && k3d cluster create joblens --servers 1 --agents 2
```
