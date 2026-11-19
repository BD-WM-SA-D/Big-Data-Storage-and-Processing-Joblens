# HƯỚNG DẪN CHẠY CHI TIẾT TOÀN BỘ DỰ ÁN JOBLENS (IT4931)

Tài liệu này hướng dẫn **dựng, vận hành, demo và kiểm tra** toàn bộ nền tảng Big Data JobLens trên k3d/Kubernetes, bám theo code hiện tại của repo (cập nhật theo commit mới nhất trên `main`).

> **Tham khảo chính:** [`deploy/README.md`](deploy/README.md), [`docs/chay-demo.md`](docs/chay-demo.md), [`docs/demo.md`](docs/demo.md), [`docs/tap-duot-demo.md`](docs/tap-duot-demo.md), [`docs/kien-truc.md`](docs/kien-truc.md)

## Mục lục

- [0. Chạy nhanh (TL;DR)](#0-chạy-nhanh-tldr)
- [1. Yêu cầu hệ thống](#1-yêu-cầu-hệ-thống)
- [2. Cài đặt môi trường phát triển](#2-cài-đặt-môi-trường-phát-triển)
- [3. Chuẩn bị repo](#3-chuẩn-bị-repo)
- [4. Dựng cụm từ đầu (Bootstrap toàn bộ)](#4-dựng-cụm-từ-đầu-bootstrap-toàn-bộ)
- [5. Dựng từng bước thủ công (tùy chọn)](#5-dựng-từng-bước-thủ-công-tùy-chọn)
- [6. Khởi động lại cụm đã có](#6-khởi-động-lại-cụm-đã-có)
- [7. Mở giao diện demo](#7-mở-giao-diện-demo)
- [8. Nạp dữ liệu và chạy pipeline](#8-nạp-dữ-liệu-và-chạy-pipeline)
- [9. Chạy streaming demo](#9-chạy-streaming-demo)
- [10. Thao tác dữ liệu tại chỗ](#10-thao-tác-dữ-liệu-tại-chỗ)
- [11. Kiểm tra và xác minh](#11-kiểm-tra-và-xác-minh)
- [12. Thí nghiệm mở rộng (benchmark, nhiễu, tối ưu join)](#12-thí-nghiệm-mở-rộng-benchmark-nhiễu-tối-ưu-join)
- [13. Quản lý RAM / Chế độ demo](#13-quản-lý-ram--chế-độ-demo)
- [14. Gỡ bỏ / Tạo lại cụm](#14-gỡ-bỏ--tạo-lại-cụm)
- [15. Sự cố thường gặp](#15-sự-cố-thường-gặp)
- [16. Ghi chép số liệu](#16-ghi-chép-số-liệu)
- [Phụ lục A. Xử lý lỗi tải Helm chart Airflow](#phụ-lục-a-xử-lý-lỗi-tải-helm-chart-airflow)

---

## 0. Chạy nhanh (TL;DR)

Dành cho máy đã cài đủ công cụ (xem mục 2). Toàn bộ lệnh chạy **từ gốc repo**.

```bash
cd ~/joblens
git clone https://github.com/BD-WM-SA-D/Big-Data-Storage-and-Processing-Joblens.git
git clone https://github.com/BD-WM-SA-D/joblens.git      # fixtures cho smoke test
cd Big-Data-Storage-and-Processing-Joblens

cp .env.example .env
uv sync
source .venv/bin/activate
export JOBLENS_PROFILE=full                              # máy 8 GB: lite

bash scripts/bootstrap-all.sh                            # ~40–60 phút lần đầu
bash deploy/smoke-e2e.sh                                 # kiểm tra toàn tuyến
bash deploy/data-tools/install.sh                        # in lại mật khẩu SQLPad/Mongo Express (đã cài ở bước 11)
bash scripts/demo-ui.sh                                  # giữ terminal mở
```

Sau đó mở http://localhost:8082 (Airflow), đăng nhập `admin` với mật khẩu:

```bash
kubectl get secret airflow-admin -n orchestration -o jsonpath='{.data.password}' | base64 -d; echo
```

Bật DAG `joblens_pipeline` (mặc định đang tạm dừng) rồi bấm **Trigger**. Chi tiết từng bước ở các mục dưới.

---

## 1. Yêu cầu hệ thống

| Cấu hình                 | Khuyến nghị                                  | Ghi chú                                                                          |
| -------------------------- | ---------------------------------------------- | --------------------------------------------------------------------------------- |
| **RAM**              | >= 16 GB (khuyến nghị)                       | 8 GB dùng `JOBLENS_PROFILE=lite`. 4 GB không đủ để dựng cụm đầy đủ. |
| **CPU**              | >= 4 cores                                     | Tốt hơn với 6–8 cores.                                                        |
| **Ổ cứng**         | >= 50–70 GB trống                            | Docker images (Spark ~2 GB), PVCs, dữ liệu VietJobs, logs.                      |
| **Hệ điều hành** | Linux/WSL2 (Ubuntu) hoặc macOS/Windows + WSL2 | Ưu tiên WSL2 Ubuntu trên Windows.                                              |
| **Docker**           | Docker Desktop/Engine đang chạy              | Cần bật WSL integration nếu dùng WSL2.                                        |

### Cài đặt WSL config (khuyến nghị)

Trên Windows, tạo file `C:\Users\<username>\.wslconfig`:

```ini
[wsl2]
memory=12GB
swap=8GB
processors=6
```

Sau đó mở PowerShell với quyền admin: `wsl --shutdown` và khởi động lại WSL.

> Máy 16 GB nên để WSL khoảng 12 GB, chừa lại cho Windows/Docker Desktop. Máy 8 GB dùng 6 GB và `JOBLENS_PROFILE=lite`.

### Công cụ cần thiết

- [Docker](https://www.docker.com/) (Docker Desktop hoặc Docker Engine)
- [k3d](https://k3d.io/) – cụm Kubernetes local
- [kubectl](https://kubernetes.io/docs/tasks/tools/) – **không** dùng bản quá cũ (cờ `--short` đã bị bỏ)
- [helm](https://helm.sh/) – >= 3.14
- [uv](https://docs.astral.sh/uv/) – quản lý Python environment
- [git](https://git-scm.com/)
- [Java 17](https://adoptium.net/) – cần cho PySpark tests (Java 21 vẫn chạy được nhưng 17 là bản khuyến nghị)

---

## 2. Cài đặt môi trường phát triển

### 2.1. Trên Ubuntu/WSL2

```bash
# k3d
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash

# kubectl – tải vào /tmp rồi cài, KHÔNG tải vào thư mục repo
cd /tmp
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm -f kubectl

# helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"     # hoặc export PATH="$HOME/.local/bin:$PATH"
```

Kiểm tra:

```bash
docker version
k3d version
kubectl version --client
helm version
uv --version
git --version
```

> **Lỗi thường gặp:** `uv: command not found` sau khi cài → uv nằm ở `~/.local/bin`, chưa có trong PATH. Chạy `source "$HOME/.local/bin/env"` hoặc thêm `export PATH="$HOME/.local/bin:$PATH"` vào `~/.bashrc`.

### 2.2. Quyền dùng Docker (WSL/Linux)

Nếu `docker ps` báo `permission denied while trying to connect to the Docker daemon socket`:

```bash
sudo usermod -aG docker $USER
newgrp docker          # áp dụng ngay trong shell hiện tại
docker ps              # phải chạy được, không cần sudo
```

Nếu vẫn lỗi, đăng xuất/đăng nhập lại WSL (hoặc `wsl --shutdown` từ PowerShell rồi mở lại).

### 2.3. Cài đặt Java 17 (cho tests)

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
```

Kiểm tra: `java -version` phải hiện `17.x`.

---

## 3. Chuẩn bị repo

### 3.1. Clone repo (cần **hai** repo cạnh nhau)

Repo môn Big Data dùng fixtures từ repo chung `joblens` cho smoke test. Clone cả hai làm **anh em cùng cấp**:

```bash
mkdir -p ~/joblens
cd ~/joblens
git clone https://github.com/BD-WM-SA-D/Big-Data-Storage-and-Processing-Joblens.git
git clone https://github.com/BD-WM-SA-D/joblens.git
cd Big-Data-Storage-and-Processing-Joblens
```

Cấu trúc đúng:

```text
~/joblens/
├── Big-Data-Storage-and-Processing-Joblens/   # repo môn Big Data (chạy lệnh ở đây)
└── joblens/                                    # fixtures, schema, contract dùng chung
```

> Nếu không muốn clone `joblens`, vẫn chạy được phần lớn pipeline, nhưng `deploy/smoke-e2e.sh` cần fixtures. Khi đó truyền đường dẫn chứa fixtures: `bash deploy/smoke-e2e.sh /duong/dan/toi/joblens`.

### 3.2. Cấu hình môi trường Python

```bash
# Từ gốc repo Big Data
cp .env.example .env
uv sync
source .venv/bin/activate        # prompt hiện (.venv) hoặc tên môi trường
```

- `.env` đã nằm trong `.gitignore` – **không commit**.
- `deploy/storage/install.sh` tự sinh khóa S3 nếu `.env` để trống.
- Có thể dùng `uv run <lệnh>` thay vì kích hoạt venv.

### 3.3. Chọn profile theo RAM

```bash
# Máy >= 16GB (mặc định)
export JOBLENS_PROFILE=full

# Máy 8GB
export JOBLENS_PROFILE=lite

# Thêm vào ~/.bashrc để tự động áp dụng
echo 'export JOBLENS_PROFILE=full' >> ~/.bashrc   # hoặc lite
source ~/.bashrc
```

| Profile  | Cụm k3d                        | Khác biệt chính                                                |
| -------- | ------------------------------- | ----------------------------------------------------------------- |
| `full` | 1 server + 2 agent              | Kafka UI, executor riêng cho Spark,daemon đầy đủ             |
| `lite` | 1 server, 0 agent, tắt Traefik | Spark chạy local mode (`EXECUTORS=0`), bỏ Kafka UI, giảm RAM |

> Đổi profile sau khi đã tạo cụm phải **xóa cụm** (`k3d cluster delete joblens`) – mất dữ liệu trong cụm.

---

## 4. Dựng cụm từ đầu (Bootstrap toàn bộ)

**Cách nhanh nhất và khuyến nghị** cho máy mới:

```bash
cd ~/joblens/Big-Data-Storage-and-Processing-Joblens
bash scripts/bootstrap-all.sh
```

Script chạy tuần tự (idempotent – chạy lại an toàn):

| Bước | Nội dung                                                                                 |
| ------ | ----------------------------------------------------------------------------------------- |
| 0      | Tạo cụm k3d `joblens` theo profile + tạo `.env`                                    |
| 1      | Strimzi 1.2.0 + Kafka 4.3.1 KRaft + 8 topic                                               |
| 2      | SeaweedFS 4.48.0 (S3) + Lakekeeper 0.12.0 + Postgres                                      |
| 3      | MongoDB 8.0 + user `joblens`                                                            |
| 4      | Trino 483 (catalog `iceberg`, `mongodb`)                                              |
| 5      | Tải VietJobs (Hugging Face) lên `s3://joblens/raw/vietjobs/`                          |
| 6      | Nạp VietJobs vào `bronze.jobs_raw` (batch)                                            |
| 7      | `bronze → silver.job_postings` (MERGE) + snapshot                                      |
| 8      | dbt build: seed 34 tỉnh, `gold.dim_location`, `gold.mart_salary_role_region` + tests |
| 9      | Grafana 13.2.4 + plugin Trino + dashboard; Kafka UI (full)                                |
| 10     | Postgres + Airflow 3.3.2 (chart 1.22.0) + DAG `joblens_pipeline`                        |
| 11     | SQLPad 7.5.7 + mongo-express 1.0.2 (sửa dữ liệu trên trình duyệt)                   |

**Thời gian:** ~40–60 phút lần đầu (tải image, tải VietJobs, build image Spark/dbt, cài Airflow và SQLPad).

Biến thể:

```bash
# Chỉ hạ tầng, không nạp dữ liệu
SKIP_DATA=1 bash scripts/bootstrap-all.sh

# Dùng ít agent hơn (ví dụ máy chật đĩa)
K3D_AGENTS=1 bash scripts/bootstrap-all.sh
```

Khi hoàn thành, bạn sẽ thấy:

```text
########## Xong ##########
```

> **Lưu ý:** `bootstrap-all.sh` **không** chạy smoke test. Sau khi xong, chạy `bash deploy/smoke-e2e.sh` (mục 5, bước 4) để kiểm tra toàn tuyến Kafka → Spark → Iceberg → Trino, rồi ghi một dòng vào [`deploy/compat.md`](deploy/compat.md).

### Nếu Airflow lỗi khi tải Helm chart

Mạng tới `archive.apache.org` có thể timeout. Xem [Phụ lục A](#phụ-lục-a-xử-lý-lỗi-tải-helm-chart-airflow). Phần còn lại của cụm vẫn đúng; chỉ bước `helm upgrade` của Airflow cần xử lý riêng.

---

## 5. Dựng từng bước thủ công (tùy chọn)

Nếu muốn theo dõi từng bước, chạy lần lượt từ gốc repo:

| Bước       | Lệnh                                                                   | Mục đích                                          |
| ------------ | ----------------------------------------------------------------------- | ---------------------------------------------------- |
| **-1** | `bash deploy/k3d/create-cluster.sh`                                   | Tạo cụm k3d `joblens`                            |
| **0**  | `cp .env.example .env && uv sync && source .venv/bin/activate`        | Cấu hình môi trường Python                      |
| **1**  | `bash deploy/kafka/install.sh`                                        | Strimzi + Kafka + 8 topic                            |
| **2**  | `bash deploy/storage/install.sh`                                      | SeaweedFS + Lakekeeper                               |
| **2b** | `bash deploy/mongodb/install.sh`                                      | MongoDB + Secret `joblens-mongo`                   |
| **3**  | `bash deploy/trino/install.sh`                                        | Trino với catalog `iceberg` + `mongodb`         |
| **4**  | `bash deploy/smoke-e2e.sh`                                            | Smoke test E2E: fixtures → Kafka → bronze → Trino |
| **5**  | `bash deploy/data/upload-vietjobs.sh`                                 | Tải VietJobs lên S3                                |
| **6**  | `DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze` | Nạp 48k tin VietJobs vào bronze                    |
| **7**  | `bash deploy/spark/submit.sh silver_job_postings`                     | bronze → silver (MERGE) + snapshot                  |
| **8**  | `bash scripts/dbt.sh build`                                           | dbt seed + gold models + 12 test dữ liệu           |
| **9**  | `bash deploy/monitoring/install.sh`                                   | Grafana + Kafka UI                                   |
| **10** | `bash deploy/airflow/install.sh`                                      | Airflow + build image Spark/dbt + DAG                |
| **11** | `bash deploy/data-tools/install.sh`                                   | SQLPad + Mongo Express (tùy chọn, cho demo)        |

> **Bắt buộc:** sau bước 4 (smoke test) thành công, thêm một dòng vào [`deploy/compat.md`](deploy/compat.md).

### Smoke test cần fixtures từ repo `joblens`

```bash
# Nếu đã clone joblens cạnh repo này:
bash deploy/smoke-e2e.sh

# Nếu để chỗ khác, truyền đường dẫn:
bash deploy/smoke-e2e.sh /duong/dan/toi/joblens
```

Kết quả đúng: bảng Trino có dòng `source = synthetic` (mỗi lần chạy gửi thêm 3 bản ghi).

> **Lỗi thường gặp:** `No such file ... fixtures/...` → chưa clone `joblens` hoặc truyền sai đường dẫn. Script mặc định tìm thư mục anh em `../joblens`.

### Về việc build image Spark/dbt

Không build tay bằng `docker build ./spark-jobs` (sai context). Luôn dùng script, vì `deploy/lib.sh` tự build đúng `-f spark-jobs/Dockerfile` với context là **gốc repo**, rồi nạp image vào k3d:

```bash
bash deploy/spark/submit.sh silver_job_postings   # tự build + import image
```

Tag image dạng `joblens/spark-jobs:0.1.0-<gitsha>` (thêm `-dirty` nếu có thay đổi chưa commit).

---

## 6. Khởi động lại cụm đã có

Nếu đã dựng cụm trước đó và máy đã tắt:

```bash
# 1. Mở Docker Desktop, đợi "Engine running"
cd ~/joblens/Big-Data-Storage-and-Processing-Joblens
git pull
uv sync
source .venv/bin/activate
k3d cluster start joblens
```

Đợi 3–5 phút, kiểm tra:

```bash
kubectl get pods -A | grep -v -E "Running|Completed"
```

✅ Chỉ còn dòng tiêu đề là sẵn sàng. Nếu còn pod lỗi sau 5 phút:

```bash
k3d cluster stop joblens && k3d cluster start joblens
```

Pod `Error`/`Terminating` cũ trong `compute` có thể xóa an toàn:

```bash
kubectl delete pod -n compute <tên-pod>
```

Máy chưa từng cài SQLPad/Mongo Express (hoặc vừa `git pull` bản mới):

```bash
bash deploy/data-tools/install.sh
```

---

## 7. Mở giao diện demo

Mở terminal và chạy (terminal này **giữ nguyên**, Ctrl+C để đóng tất cả):

```bash
bash scripts/demo-ui.sh
```

| Giao diện                | URL                                                  | Tài khoản                           | Mục đích                         |
| ------------------------- | ---------------------------------------------------- | ------------------------------------- | ----------------------------------- |
| **Grafana**         | http://localhost:3000/d/joblens-demo                 | Không cần đăng nhập để xem     | Dashboard tổng quan Big Data       |
| **Kafka UI** (full) | http://localhost:8081/ui/clusters/joblens/all-topics | Không cần đăng nhập (chỉ đọc) | Xem topics/messages                 |
| **Airflow**         | http://localhost:8082/dags/joblens_pipeline          | `admin` + mật khẩu                | Điều phối pipeline               |
| **Trino UI**        | http://localhost:8080/ui/                            | Nhập user bất kỳ (dev)             | Truy vấn SQL                       |
| **Lakekeeper UI**   | http://localhost:8181/ui/                            | Không cần đăng nhập (dev)        | Quản lý Iceberg catalog           |
| **SeaweedFS**       | http://localhost:8888/buckets/joblens/               | Không cần đăng nhập (dev)        | Xem file trên S3                   |
| **SQLPad**          | http://localhost:3001                                | `admin@joblens.local` + mật khẩu  | Soạn/chạy SQL trên trình duyệt |
| **Mongo Express**   | http://localhost:8083/db/joblens/                    | `admin` + mật khẩu                | Xem/sửa document MongoDB           |

### Lấy mật khẩu

```bash
# Airflow
kubectl get secret airflow-admin -n orchestration -o jsonpath='{.data.password}' | base64 -d; echo

# Grafana
kubectl get secret grafana-admin -n monitoring -o jsonpath='{.data.password}' | base64 -d; echo

# SQLPad / Mongo Express: chạy lại script để in mật khẩu (an toàn, không tạo lại secret)
bash deploy/data-tools/install.sh
```

> Không chiếu mật khẩu lên màn hình khi demo. Chạy lệnh lấy mật khẩu ở tab riêng.

### Mở Spark UI (khi có job Spark đang chạy)

```bash
bash scripts/demo-ui.sh spark
```

Cổng 4040, tự bám theo driver mới nhất trong namespace `compute` (job streaming hoặc task Airflow). Mở tab **Structured Streaming** khi chạy `bronze_jobs`.

---

## 8. Nạp dữ liệu và chạy pipeline

### 8.1. Kiểm tra dữ liệu hiện có

```bash
bash scripts/trino.sh -f scripts/demo-queries.sql
```

Kết quả mong đợi: số liệu bronze/silver/gold, tỷ lệ có địa điểm/có lương.

Truy vấn nhanh:

```bash
bash scripts/trino.sh "SELECT count(*) FROM iceberg.bronze.jobs_raw"
bash scripts/trino.sh "SELECT count(*) FROM iceberg.silver.job_postings"
bash scripts/trino.sh "SELECT count(*) FROM iceberg.gold.mart_salary_role_region"
```

> Dùng `scripts/trino.sh` – script tự tìm pod coordinator theo label, không cần biết tên container (trước đây `kubectl exec ... -c trino` dễ sai vì container tên `trino-coordinator`).

### 8.2. Chạy DAG Airflow

1. Mở http://localhost:8082 → đăng nhập `admin`.
2. Vào **DAGs** → `joblens_pipeline` → bật công tắc **Pause** sang trạng thái chạy (nếu đang tạm dừng).
3. Bấm **Trigger DAG** (▶).

Hoặc bằng CLI:

```bash
kubectl exec -n orchestration statefulset/airflow-scheduler -c scheduler -- \
  airflow dags unpause joblens_pipeline
kubectl exec -n orchestration statefulset/airflow-scheduler -c scheduler -- \
  airflow dags trigger joblens_pipeline
```

> **Lưu ý về `unpause`:** lần đầu, lệnh trả bảng `is_paused | True` (giá trị **trước** khi đổi). Lần chạy lại sẽ báo `No paused DAGs were found`; kiểm tra bằng `airflow dags list | grep joblens_pipeline` – cột `Paused` phải là `False`.

DAG chạy theo thứ tự:

```text
silver_job_postings → gx_validate_silver → ┬→ dbt_build_gold
                                            └→ spark_analytics → iceberg_maintenance
```

- `spark_analytics` chạy **song song** với `dbt_build_gold` (dbt nhẹ), nhưng `iceberg_maintenance` chỉ chạy sau `spark_analytics`.
- Lịch: `0 * * * *` (mỗi giờ), `catchup=False`, `max_active_runs=1`.
- DAG mặc định **paused** sau khi cài. Khi demo, nên trigger xong rồi tạm dừng lại để lượt chạy theo giờ không chen vào.

Xem tiến trình ở tab **Graph**/**Gantt**; log Spark hiện thẳng trên Airflow vì task Spark chạy client mode.

### 8.3. Chạy thủ công từng job

```bash
# Silver từ bronze
bash deploy/spark/submit.sh silver_job_postings

# Kiểm tra chất lượng bằng Great Expectations (fail sẽ trả exit code 1)
bash deploy/spark/submit.sh gx_silver

# Phân tích Spark + MLlib (ghi MongoDB analytics_*)
bash deploy/spark/submit.sh analytics_spark

# Bảo trì Iceberg (rewrite data files, manifests, expire snapshots)
bash deploy/spark/submit.sh maintenance_iceberg

# dbt build
bash scripts/dbt.sh build
```

Tùy chọn bộ nhớ (máy yếu):

```bash
DRIVER_MEMORY=1g EXECUTOR_MEMORY=1g bash deploy/spark/submit.sh silver_job_postings
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh analytics_spark   # local mode
```

> `EXECUTORS=0` chạy Spark local mode trong một pod duy nhất (tiết kiệm ~0.7–1 GB RAM), mã job và bảng giống hệt.

---

## 9. Chạy streaming demo

**Quan trọng:** với máy 16 GB (WSL ~12 GB) hoặc 8 GB, **không chạy streaming và Airflow cùng lúc**. `scripts/demo-mode.sh` lo việc chuyển chế độ.

### 9.1. Chuyển sang chế độ streaming

```bash
bash scripts/demo-mode.sh streaming
```

Script sẽ: tạm dừng DAG `joblens_pipeline`, đợi các task Airflow đang chạy xong (không giết giữa chừng), rồi tắt scheduler + dag-processor (giải phóng ~1 GB). Giao diện Airflow vẫn mở được.

### 9.2. Chạy các job streaming

Mở 3 terminal riêng biệt (T2, T3, T4), mỗi tab `cd` vào gốc repo:

**T2 – Streaming nạp bronze từ Kafka (micro-batch 10 giây):**

```bash
DRIVER_MEMORY=768m EXECUTOR_MEMORY=768m bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"
```

**T3 – Streaming có trạng thái theo tỉnh (watermark, ghi MongoDB):**

```bash
DRIVER_MEMORY=768m EXECUTORS=0 bash deploy/spark/submit.sh stream_province_windows
```

**T4 – Replay dữ liệu VietJobs vào Kafka (có thể tiêm tin trễ để thử watermark):**

```bash
bash deploy/data/replay-vietjobs.sh 3000 50 --late-every 10
```

Tham số replay: `LIMIT RATE [--late-every N]` – mặc định 3.000 tin, 50 tin/giây (~1 phút).

### 9.3. Theo dõi

- **Kafka UI:** messages chảy vào `jobs.raw.vietjobs.v1`.
- **Grafana:** "Tin nạp vào bronze theo 10 giây", "Tin mới theo tỉnh – cửa sổ 5 phút".
- **Spark UI** (`bash scripts/demo-ui.sh spark`): Structured Streaming, batch duration, state.
- **Log T3:** tìm dòng `tiến độ batch ... bỏ do trễ (watermark) ...`.
- **MongoDB:** `bash scripts/mongo.sh 'db.province_windows.find().limit(3)'`.

### 9.4. Dừng streaming và bật lại Airflow

```bash
# Ctrl+C ở T2 và T3 (chỉ dừng xem log; job trong cụm vẫn chạy)
bash scripts/demo-mode.sh airflow
```

Lệnh trên tự dừng job streaming (`bronze_jobs`, `stream_province_windows`), bật lại Airflow và **bỏ tạm dừng** DAG.

---

## 10. Thao tác dữ liệu tại chỗ

Ba cách, tùy buổi demo:

### 10.1. Terminal – `scripts/demo-data.sh`

```bash
# SQL trên Iceberg: CTAS, INSERT, UPDATE, DELETE, snapshot, time travel
bash scripts/demo-data.sh sql

# NoSQL trên MongoDB: insert/find/update/delete/aggregate + Trino đọc/join
bash scripts/demo-data.sh nosql

# Gửi 1 tin demo qua Kafka → bronze → silver (chuẩn hóa)
bash scripts/demo-data.sh kafka "Kỹ sư dữ liệu (tin demo)" "15 - 25 triệu" "Đà Nẵng"

# Dọn dẹp dữ liệu demo
bash scripts/demo-data.sh clean
```

Mỗi bước dừng chờ Enter để bạn giải thích (đặt `NONSTOP=1` để chạy một mạch).

Vùng an toàn: chỉ ghi vào Iceberg schema `sandbox`, MongoDB collection `sandbox_jobs`, và topic `jobs.raw.vietjobs.v1` với `source=synthetic`, `source_job_id` bắt đầu bằng `demo-`. Không đụng bảng bronze/silver/gold.

### 10.2. Trình duyệt – SQLPad và Mongo Express

```bash
bash deploy/data-tools/install.sh     # in lại URL + mật khẩu
```

- SQLPad http://localhost:3001: 8 câu lệnh mẫu trong **Queries** (tạo bảng, insert, update, delete, time travel, MongoDB, dữ liệu thật, dọn dẹp).
- Mongo Express http://localhost:8083/db/joblens/: thêm/sửa/xóa document trực quan.

### 10.3. MongoDB shell

```bash
# Phiên tương tác (user ứng dụng joblens, không phải root)
bash scripts/mongo.sh

# Chạy một lệnh
bash scripts/mongo.sh 'db.province_windows.countDocuments()'
```

> Collection do pipeline ghi (`province_windows`, `analytics_*`) sẽ bị job ghi đè. Thử nghiệm trên collection riêng, ví dụ `sandbox_jobs`.

---

## 11. Kiểm tra và xác minh

### 11.1. Pods

```bash
kubectl get pods -A
```

Tất cả pod quan trọng phải `Running` hoặc `Completed`. Xem nhanh pod chưa ổn:

```bash
kubectl get pods -A | grep -v -E "Running|Completed"
```

### 11.2. Topics Kafka (8 topic)

```bash
kubectl get kafkatopics -n ingest
```

| Topic                          | Partitions |
| ------------------------------ | ---------- |
| `jobs-raw-topcv-v1`          | 6          |
| `jobs-raw-itviec-v1`         | 6          |
| `reviews-raw-itviec-v1`      | 3          |
| `events-user-v1`             | 6          |
| `dlq-parse-errors-v1`        | 1          |
| `app-alert-subscriptions-v1` | 3          |
| `alerts-job-match-v1`        | 3          |
| `jobs-raw-vietjobs-v1`       | 6          |

### 11.3. Tests và pre-commit

```bash
uv run pytest -q
uv run pytest tests/test_normalize.py -v
uv run pytest tests/test_noise.py -v

pre-commit run --all-files
```

Kết quả mong đợi: `passed`, pre-commit không sửa file nào. Test Spark cần Java 17 và có thể mất vài phút.

### 11.4. Dữ liệu qua Trino

```bash
bash scripts/trino.sh "SELECT count(*) FROM iceberg.bronze.jobs_raw"
bash scripts/trino.sh "SELECT count(*) FROM iceberg.silver.job_postings"
bash scripts/trino.sh "SELECT * FROM iceberg.gold.mart_salary_role_region LIMIT 5"
bash scripts/trino.sh "SELECT * FROM mongodb.joblens.province_windows LIMIT 5"
bash scripts/trino.sh -f scripts/demo-queries.sql
```

Kiểm tra ánh xạ địa điểm (34 tỉnh sau sáp nhập 2025):

```bash
bash scripts/trino.sh -f scripts/check-locations.sql
```

### 11.5. GX results

Kết quả Great Expectations lưu trên S3 tại `s3://joblens/quality/gx/silver_job_postings/`, xem qua SeaweedFS UI. Nếu GX fail, task trả exit code 1 và dbt không chạy (gold giữ bản cũ) – đây là hành vi đúng.

---

## 12. Thí nghiệm mở rộng (benchmark, nhiễu, tối ưu join)

Các thí nghiệm trong yêu cầu môn học. **Chỉ chạy khi cụm khỏe** và nên chạy riêng, không đồng thời với demo.

### 12.1. Batch vs streaming

```bash
bash scripts/benchmark-batch-vs-streaming.sh 5000 100 "10 seconds"
```

Script tự tạm dừng Airflow, chạy đường batch (VietJobs → bronze) và đường streaming (replay Kafka → micro-batch), đo độ trễ, thông lượng, số file, CPU-giây/GB-giờ, độ đúng. Số đo in ra bảng Markdown; chép vào [`benchmarks/batch_vs_streaming.md`](benchmarks/batch_vs_streaming.md) và ghi một dòng `deploy/compat.md`.

### 12.2. Thí nghiệm nhiễu

```bash
# 1) Tiêm nhiễu 4 mức (0/10/20/40%), cùng seed để tái lập
for r in 0 0.1 0.2 0.4; do
  EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab inject --rate $r --seed 42
done

# 2) Làm sạch qua đúng mã pipeline (2 chế độ)
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab clean

# 3) Đo so với đáp án
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab evaluate
```

Chi tiết và cách đọc kết quả: [`docs/thi-nghiem-nhieu.md`](docs/thi-nghiem-nhieu.md), số liệu: [`benchmarks/noise_robustness.md`](benchmarks/noise_robustness.md).

### 12.3. Tối ưu nhiều join

Xem [`docs/thi-nghiem-spark.md`](docs/thi-nghiem-spark.md) mục E và [`benchmarks/join_optimization.md`](benchmarks/join_optimization.md) (AQE bật/tắt, broadcast join, viết lại thứ tự join).

### 12.4. Các lượt đo khác

- Streaming `--output-mode append`: xem `docs/thi-nghiem-spark.md` mục B.
- Số file bronze trước/sau `iceberg_maintenance`: mục C.
- Kết quả Spark analytics + MLlib: [`benchmarks/spark_analytics.md`](benchmarks/spark_analytics.md).

---

## 13. Quản lý RAM / Chế độ demo

### 13.1. Xem trạng thái RAM

```bash
bash scripts/demo-mode.sh status
```

In RAM từng node, 12 pod dùng nhiều RAM nhất, và bộ nhớ WSL.

### 13.2. Chuyển đổi chế độ

```bash
# Chế độ streaming: tạm dừng DAG, đợi task xong, tắt scheduler + dag-processor
bash scripts/demo-mode.sh streaming

# Chế độ Airflow: dừng job streaming, bật lại Airflow, bỏ tạm dừng DAG
bash scripts/demo-mode.sh airflow
```

### 13.3. Mẹo tiết kiệm RAM (máy 8 GB)

- Chỉ chạy **một** job Spark cùng lúc.
- Dừng streaming trước khi chạy DAG Airflow (hoặc dùng `demo-mode.sh`).
- Dùng `EXECUTORS=0` cho streaming job.
- Dùng `JOBLENS_PROFILE=lite` ngay từ đầu (1 node, không Kafka UI).
- Chỉ mở tab trình duyệt cần thiết; đóng ứng dụng nặng trên Windows.
- Tắt Airflow khi không dùng:

```bash
kubectl scale -n orchestration statefulset/airflow-scheduler --replicas=0
kubectl scale -n orchestration deployment/airflow-dag-processor deployment/airflow-api-server --replicas=0
```

Bật lại:

```bash
kubectl scale -n orchestration statefulset/airflow-scheduler --replicas=1
kubectl scale -n orchestration deployment/airflow-dag-processor deployment/airflow-api-server --replicas=1
```

---

## 14. Gỡ bỏ / Tạo lại cụm

### Dừng cụm (giữ dữ liệu)

```bash
k3d cluster stop joblens
```

### Khởi động lại

```bash
k3d cluster start joblens
```

### Xóa và tạo lại cụm (MẤT TOÀN BỘ DỮ LIỆU TRONG CỤM)

**Chỉ dùng khi cần làm mới hoàn toàn:**

```bash
k3d cluster delete joblens
bash scripts/bootstrap-all.sh
```

Image Docker vẫn còn trong Docker Desktop; lần bootstrap sau nhanh hơn.

### Dọn dẹp dữ liệu tải về (không nằm trong git)

```bash
rm -rf data/vietjobs        # CSV VietJobs đã tải
```

---

## 15. Sự cố thường gặp

| Triệu chứng                                 | Nguyên nhân                                                   | Cách xử lý                                                                                               |
| --------------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `uv: command not found`                     | `~/.local/bin` chưa có trong PATH                           | `source "$HOME/.local/bin/env"` hoặc thêm PATH vào `~/.bashrc`                                       |
| `permission denied ... docker.sock`         | User chưa thuộc group `docker`                              | `sudo usermod -aG docker $USER && newgrp docker`                                                          |
| `kubectl` báo lỗi cờ `--short`         | kubectl quá cũ                                                | Cài lại kubectl mới (mục 2.1)                                                                           |
| Repo có file `kubectl` ở gốc             | Tải nhầm bằng `curl -LO` trong repo                        | `rm -f kubectl`; **không commit** file này                                                        |
| `connection refused` / `connection reset` | Cụm chưa khởi động xong                                    | `k3d cluster stop joblens && k3d cluster start joblens`, đợi 2–3 phút                                 |
| Pod `Pending`                               | Thiếu RAM/CPU                                                  | `bash scripts/demo-mode.sh status`, đóng ứng dụng Windows, giảm replicas                             |
| Pod `OOMKilled`                             | Hết bộ nhớ                                                   | Giảm `DRIVER_MEMORY`, dùng `EXECUTORS=0`, hoặc profile `lite`                                      |
| `smoke-e2e.sh` báo thiếu fixtures         | Chưa clone `joblens` cạnh repo                              | Clone `joblens` hoặc truyền đường dẫn: `bash deploy/smoke-e2e.sh /path/to/joblens`                |
| Trino CLI báo sai container                  | Container tên `trino-coordinator`                            | Dùng `bash scripts/trino.sh "…"` thay vì `kubectl exec -c trino`                                     |
| `docker build ./spark-jobs` lỗi context    | Sai Dockerfile/context                                          | Dùng `deploy/spark/submit.sh` hoặc `deploy/lib.sh` (`-f spark-jobs/Dockerfile` + context gốc repo) |
| Helm báo timeout tải chart Airflow          | `archive.apache.org` chậm/chặn                              | Xem [Phụ lục A](#phụ-lục-a-xử-lý-lỗi-tải-helm-chart-airflow)                                         |
| Airflow pod kẹt `Init:0/1`                 | Job migration/create-user chưa xong                            | Đợi 3–7 phút; xem `kubectl get jobs -n orchestration`                                                 |
| DAG vẫn paused sau `unpause`               | Giá trị `True` là trạng thái **trước** khi đổi | Chạy lại lệnh hoặc bật trên UI; kiểm tra `Paused=False`                                            |
| Airflow task `Forbidden`                    | Thiếu RBAC                                                     | Chạy lại `bash deploy/airflow/install.sh`                                                               |
| DAG chạy bản cũ sau sửa code              | ConfigMap chưa cập nhật                                      | Chạy lại `bash deploy/airflow/install.sh` (tự restart scheduler/dag-processor)                         |
| Grafana "Plugin not found"                    | Chưa tải xong plugin Trino                                    | Đợi 1–2 phút, reload; nếu vẫn lỗi: `kubectl rollout restart -n monitoring deploy/grafana`          |
| Grafana "Table does not exist"                | Chưa chạy pipeline tương ứng                               | Chạy DAG/job để tạo bảng; đổi khoảng thời gian về "Last 15 minutes"                               |
| Trino lỗi MongoDB catalog                    | Chưa cài MongoDB/catalog                                      | `bash deploy/mongodb/install.sh && bash deploy/trino/install.sh`                                          |
| Kafka `CrashLoopBackOff`                    | Tắt đột ngột                                                | Đợi 3–5 phút tự hồi phục; **không xóa PVC**                                                  |
| SQLPad/Mongo Express chưa có                | Chưa chạy bước 11                                           | `bash deploy/data-tools/install.sh`                                                                       |
| `demo-ui.sh` báo port đang dùng          | Port-forward cũ chưa đóng                                   | `pkill -f "kubectl port-forward"` rồi chạy lại                                                         |
| Streaming đọc lại toàn bộ dữ liệu      | Đã xóa checkpoint PVC                                        | Không xóa PVC `compute/spark-checkpoints` trừ khi muốn reset                                          |
| GX fail, dbt không chạy                     | Dữ liệu silver không đạt ngưỡng                          | Xem JSON trên `s3://joblens/quality/gx/...`; sửa dữ liệu/logic rồi chạy lại                        |

### Kiểm tra logs

```bash
# Pod cụ thể
kubectl logs -n <namespace> <pod-name> --tail=100

# Job Spark
kubectl logs -n compute $(kubectl get pods -n compute -o name | grep bronze-jobs | tail -1)

# Task Airflow (pod task trong compute)
kubectl logs -n compute $(kubectl get pods -n compute -o name | grep silver-job-postings | tail -1)

# Scheduler Airflow
kubectl logs -n orchestration statefulset/airflow-scheduler -c scheduler --tail=100
```

---

## 16. Ghi chép số liệu

Sau mỗi buổi demo/chốt số liệu, cập nhật [`deploy/compat.md`](deploy/compat.md):

```markdown
| 2026-XX-XX | <commit-sha> | – | – | joblens@<sha> | – | Ghi chú với số đo thật: thời gian chạy, số lượng dòng, kết quả benchmark... |
```

Các số liệu quan trọng:

- Số lượng tin bronze/silver/gold.
- Kết quả benchmark batch vs streaming (`benchmarks/batch_vs_streaming.md`).
- Kết quả Spark analytics + MLlib (`benchmarks/spark_analytics.md`).
- Kết quả tối ưu join (`benchmarks/join_optimization.md`).
- Độ bền nhiễu (`benchmarks/noise_robustness.md`).
- Kết quả GX (15/15 pass/fail).
- Thời gian chạy DAG.
- Số file Iceberg trước/sau maintenance.
- Hiệu năng streaming (watermark drops).

---

## Phụ lục A. Xử lý lỗi tải Helm chart Airflow

`deploy/airflow/install.sh` cài chart Apache Airflow 1.22.0 từ `https://airflow.apache.org` (chuyển tiếp tới `archive.apache.org`). Mạng một số nơi timeout:

```text
Error: ... airflow-1.22.0.tgz: context deadline exceeded
```

Script đã tạo xong secret, Postgres, ConfigMap DAG và build image trước bước Helm, nên chỉ cần xử lý riêng bước cài chart.

### A.1. Cách 1 – tải chart thủ công rồi sửa tạm một dòng (khuyến nghị)

Tải file `.tgz` bằng trình duyệt/`curl` từ một nguồn tin cậy (thử lần lượt):

```bash
curl -fL --retry 5 --retry-delay 3 -o /tmp/airflow-1.22.0.tgz \
  https://archive.apache.org/dist/airflow/helm-chart/1.22.0/airflow-1.22.0.tgz

# Nếu archive.apache.org lỗi, thử mirror:
curl -fL --retry 5 --retry-delay 3 -o /tmp/airflow-1.22.0.tgz \
  https://downloads.apache.org/airflow/helm-chart/1.22.0/airflow-1.22.0.tgz
```

Kiểm tra file tải đủ (khoảng vài trăm KB):

```bash
ls -lh /tmp/airflow-1.22.0.tgz
helm show chart /tmp/airflow-1.22.0.tgz | head
```

Sửa **tạm** dòng Helm trong `deploy/airflow/install.sh`:

```bash
# Từ:
helm upgrade --install airflow apache-airflow/airflow --version "$AIRFLOW_CHART_VERSION" -n "$NS" \
# Thành:
helm upgrade --install airflow /tmp/airflow-1.22.0.tgz -n "$NS" \
```

Rồi chạy lại script:

```bash
bash deploy/airflow/install.sh
```

Sau khi cài xong, **hoàn nguyên** dòng đó về bản gốc để lần sau vẫn cài từ repo chuẩn.

### A.2. Cách 2 – cài bằng lệnh rời (không sửa file)

Nếu script đã chạy và dừng đúng ở bước Helm, chạy các lệnh sau từ gốc repo:

```bash
. deploy/lib.sh

SPARK_IMAGE="$(spark_image)"
DBT_IMAGE="$(image_tag joblens/dbt 0.1.0 dbt)"
VARS="$(mktemp --suffix=.yaml)"
cat > "$VARS" <<EOF
env:
  - name: AIRFLOW_VAR_JOBLENS_SPARK_IMAGE
    value: "${SPARK_IMAGE}"
  - name: AIRFLOW_VAR_JOBLENS_DBT_IMAGE
    value: "${DBT_IMAGE}"
  - name: AIRFLOW_VAR_JOBLENS_SPARK_EXECUTORS
    value: "$([ "$PROFILE" = lite ] && echo 0 || echo 1)"
EOF

kubectl delete job -n orchestration airflow-run-airflow-migrations airflow-create-user --ignore-not-found
helm upgrade --install airflow /tmp/airflow-1.22.0.tgz -n orchestration \
  -f deploy/airflow/values.yaml $(profile_values deploy/airflow) -f "$VARS" \
  --set-string createUserJob.defaultUser.password="$(kubectl get secret airflow-admin -n orchestration -o jsonpath='{.data.password}' | base64 -d)" \
  --wait --timeout 15m
rm -f "$VARS"

kubectl rollout restart statefulset/airflow-scheduler -n orchestration
kubectl rollout restart deployment/airflow-dag-processor -n orchestration
kubectl apply -f deploy/airflow/rbac.yaml
```

> Nếu chưa từng chạy `deploy/airflow/install.sh`, chạy nó một lần trước (nó sẽ dừng ở bước Helm) để tạo secret/Postgres/ConfigMap, rồi mới dùng cách này.

### A.3. Kiểm tra sau khi cài

```bash
kubectl get pods -n orchestration
kubectl get jobs -n orchestration
kubectl get secret airflow-admin -n orchestration -o jsonpath='{.data.password}' | base64 -d; echo
```

Chờ các pod `Running`, job `Completed`, rồi mở http://localhost:8082 và bật DAG `joblens_pipeline`.

---

## Checklist trước khi báo "đã chạy xong"

- [ ] `kubectl get pods -A | grep -v -E "Running|Completed"` chỉ còn dòng tiêu đề.
- [ ] `bash deploy/smoke-e2e.sh` có dòng `source = synthetic`.
- [ ] `bash scripts/trino.sh "SELECT count(*) FROM iceberg.silver.job_postings"` > 0.
- [ ] `bash scripts/dbt.sh build` pass toàn bộ test.
- [ ] `bash deploy/spark/submit.sh gx_silver` exit code 0.
- [ ] Airflow UI mở được, `joblens_pipeline` chạy xanh 5 task.
- [ ] `uv run pytest -q` pass; `pre-commit run --all-files` không đổi file.
- [ ] Đã ghi một dòng vào `deploy/compat.md` với số đo thật.

---

## Tài liệu tham khảo

- [Kiến trúc hệ thống](docs/kien-truc.md) – sơ đồ luồng dữ liệu, triển khai
- [Chạy demo từng bước](docs/chay-demo.md) – dùng trong buổi demo
- [Kịch bản demo](docs/demo.md) – hướng dẫn trình bày
- [Tập dượt demo](docs/tap-duot-demo.md) – từng bước chi tiết, xử lý lỗi
- [Yêu cầu môn học](docs/yeu-cau-mon-hoc.md) – đối chiếu IT4931
- [Thí nghiệm nhiễu](docs/thi-nghiem-nhieu.md)
- [Các lượt đo Spark](docs/thi-nghiem-spark.md)
- [Kế hoạch mở rộng](docs/ke-hoach-mo-rong.md)
- [ADR Lambda/Kappa](docs/adr/0001-lambda-vs-kappa.md)
- [Deploy README](deploy/README.md) – hướng dẫn triển khai kỹ thuật
- [Cloud/Codespaces](docs/cloud-codespaces.md) – chạy trên GitHub Codespaces
- [Benchmarks](benchmarks/) – số liệu batch vs streaming, join, nhiễu, analytics

---

**Lưu ý quan trọng:**

- Luôn chạy lệnh từ **gốc repo** Big Data.
- Không commit secrets (`.env`, keys, tokens), dữ liệu crawl/CSV, output notebook.
- Với máy 8–16 GB, tuân thủ "không chạy streaming và Airflow cùng lúc" – dùng `scripts/demo-mode.sh`.
- Chỉ nói số liệu thật trong buổi demo; ghi đầy đủ vào `deploy/compat.md`.
- Topic Kafka chỉ tạo bằng `KafkaTopic` CRD trong `deploy/`, không dùng CLI.
- Không dùng tag `latest` cho image/thư viện; phiên bản pin nằm ở repo `joblens/versions.md`.
