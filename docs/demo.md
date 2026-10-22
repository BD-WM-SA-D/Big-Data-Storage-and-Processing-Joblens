# Kịch bản demo môn Big Data (IT4931) – nền tảng JobLens

Mục tiêu: trong ~15 phút cho người xem **thấy tận mắt** dữ liệu được **di chuyển, lưu trữ, xử lý** thế nào trên nền
tảng tự dựng (Kafka → Spark → Iceberg → Trino/dbt, điều phối bằng Airflow, chạy trên Kubernetes), bằng dữ liệu thật
VietJobs (48.092 tin), cả đường **batch** lẫn **streaming**. Phần lớn buổi demo diễn ra trên **trình duyệt**; terminal
chỉ dùng để bấm chạy job.

> Số liệu nói trong buổi demo phải là số đo thật từ lần chạy hôm đó (quy tắc repo: không bịa số đo).
> Ghi lại vào `deploy/compat.md` sau khi chạy.

## 0. Giao diện dùng trong buổi demo

Tất cả mở bằng một lệnh `bash scripts/demo-ui.sh` (port-forward ra `localhost`, trình duyệt Windows mở thẳng được):

| Giao diện | Địa chỉ | Cho thấy gì | Bài giảng |
|---|---|---|---|
| **Grafana** – dashboard "JobLens – Big Data demo" | http://localhost:3000 | Số dòng bronze/silver, tốc độ nạp theo 10 giây, biểu đồ gold, snapshot và số file Iceberg – đọc thẳng bảng Iceberg qua Trino, tự làm mới 5 giây | L03, L04, L14 |
| **Kafka UI** (chỉ đọc) | http://localhost:8081 | Topic, 6 partition, số message tăng khi replay, xem từng message JSON | L08 |
| **Spark UI** (`bash scripts/demo-ui.sh spark`) | http://localhost:4040 | Tab *Structured Streaming*: input rate, thời gian mỗi micro-batch; tab *SQL*: kế hoạch `MERGE INTO` | L10, L11 |
| **Airflow** | http://localhost:8082 | DAG `joblens_pipeline` dạng đồ thị, trạng thái từng task, log Spark/dbt, Gantt | – |
| **Trino UI** | http://localhost:8080 | Các truy vấn Grafana/dbt gửi tới: thời gian, số dòng, số split | – |
| **SeaweedFS** (filer) | http://localhost:8888/buckets/joblens/warehouse/ | File Parquet theo phân vùng + `metadata/*.json` của Iceberg – "bảng" thật ra là file trên object storage | L03, L04 |
| **Lakekeeper** | http://localhost:8181/ui/ | Iceberg REST catalog: warehouse, namespace, bảng, snapshot | L04 |
| k9s (terminal, tùy chọn) | `k9s` | Pod của từng tầng trong từng namespace, pod executor Spark sinh ra rồi mất | L01 |

Đăng nhập: Grafana xem không cần đăng nhập; Airflow `admin` + mật khẩu do `deploy/airflow/install.sh` in ra;
Trino nhập user bất kỳ.

## 1. Cài thêm (một lần, sau khi đã có pipeline theo `deploy/README.md` bước 0–8)

```bash
cd ~/joblens/Big-Data-Storage-and-Processing-joblens
git pull && uv sync
bash deploy/monitoring/install.sh     # Grafana + plugin Trino + dashboard, Kafka UI (lần đầu ~2 phút, cần Internet)
bash deploy/airflow/install.sh        # Postgres + Airflow 3.3.2 + DAG joblens_pipeline (lần đầu ~5–10 phút)
```

Tùy chọn – k9s để xem cụm trực quan trong terminal:

```bash
curl -sL https://github.com/derailed/k9s/releases/download/v0.50.18/k9s_Linux_amd64.tar.gz | tar xz -C ~/.local/bin k9s
```

**RAM (máy 16 GB).** Cấu hình trong repo đã thu nhỏ cho máy 16 GB: Trino một node, Kafka heap 512 MB, Airflow
LocalExecutor, replay Kafka bằng producer Python (không cần Spark). Lúc nhàn rỗi cả cụm ~8 GB; lúc chạy streaming
~10 GB. Cho WSL 12 GB:

1. Tạo `C:\Users\Admin\.wslconfig`:
   ```
   [wsl2]
   memory=12GB
   swap=8GB
   ```
2. PowerShell: `wsl --shutdown`, mở lại Ubuntu (Docker Desktop tự bật lại cụm k3d), đợi `kubectl get pods -A` hết
   `Pending`/`ContainerCreating`.
3. Trong buổi demo **không chạy streaming và Airflow cùng lúc** – `scripts/demo-mode.sh` chuyển giữa hai chế độ
   (mục 3). Đóng bớt ứng dụng Windows khác, trình duyệt chỉ mở các tab cần thiết.
4. Kiểm tra bất cứ lúc nào: `bash scripts/demo-mode.sh status` (RAM từng node, pod dùng nhiều RAM nhất).

## 2. Chuẩn bị (trước buổi demo ít nhất 1 ngày)

1. Dữ liệu đã có trong bronze/silver/gold (deploy/README.md bước 5–8). Kiểm tra:
   `bash scripts/trino.sh -f scripts/demo-queries.sql`.
2. Chạy thử DAG một lần: Airflow → `joblens_pipeline` → bật công tắc (unpause) → **Trigger**. Ba task phải xanh.
   Ghi lại thời gian chạy từng task (tab *Gantt*) – dùng làm số liệu trên slide.
3. Mở Grafana, kiểm tra mọi ô có số. Ô nào báo lỗi → xem mục 5.
4. Tập một lượt mục 3, **quay màn hình** phần streaming và phần Airflow làm phương án dự phòng; chụp màn hình từng tab.

## 3. Kịch bản trình bày

Trước giờ demo 15 phút:

- Terminal A: `bash scripts/demo-ui.sh` (để yên, không đóng).
- Terminal D: `bash scripts/demo-ui.sh spark` (tự nối Spark UI vào driver mới nhất).
- Trình duyệt mở sẵn các tab theo thứ tự: Grafana → Kafka UI (topic `jobs.raw.vietjobs.v1`) → Spark UI → Airflow →
  Trino UI → SeaweedFS. Grafana chọn khoảng thời gian **Last 15 minutes**, làm mới **5s**.
- Terminal B, C để trống cho phần streaming.
- Chạy `bash scripts/demo-mode.sh streaming` (tạm tắt scheduler Airflow; giao diện Airflow vẫn mở được).

| Phút | Phần | Làm gì trên màn hình | Ý chính |
|---|---|---|---|
| 0–2 | Kiến trúc | Sơ đồ `joblens/docs/project-plan/03`; `k9s` (gõ `:ns`) – mỗi tầng một namespace: `ingest` (Kafka), `lakehouse` (SeaweedFS + Lakekeeper), `compute` (Spark), `serving` (Trino), `orchestration` (Airflow), `monitoring` | 4 tầng Store/Move/Process/Query (L01); mọi thứ chạy trên K8s |
| 2–4 | Lưu trữ + batch (đường A) | Grafana: ô *Dòng trong bronze*, *Tin trong silver*, biểu đồ theo topic có `backfill.vietjobs`. Tab SeaweedFS: mở `warehouse/…/jobs_raw/` → thư mục `data/` theo phân vùng ngày + nguồn, file Parquet; `metadata/` có `*.metadata.json`, manifest | Iceberg = Parquet + metadata trên object storage (L03, L04); phân vùng `days(ingest_ts), source`; job `vietjobs --sink bronze` nạp 48k tin một lần |
| 4–8 | Streaming (đường B) – mục 3.1 | Terminal B chạy job streaming; tab Spark UI → *Structured Streaming*. Terminal C replay 3.000 tin. Kafka UI: số message của `jobs.raw.vietjobs.v1` tăng, xem một message. Grafana: cột `jobs.raw.vietjobs.v1` mọc lên mỗi 10 giây, *Dòng trong bronze* tăng, *Số file dữ liệu bronze* tăng | Kafka 6 partition, key = `source_job_id` (L08); micro-batch 10 giây, checkpoint offset trên PVC → exactly-once nhờ commit Iceberg (L10, L11). Kafka UI không hiện lag vì Spark giữ offset trong checkpoint, không commit về Kafka |
| 8–11 | Điều phối (Airflow) – mục 3.2 | `bash scripts/demo-mode.sh airflow` (dừng streaming, bật lại scheduler). Airflow: đồ thị DAG → **Trigger**; task `silver_job_postings` chuyển xanh, mở log (log Spark), tab Spark UI → *SQL* thấy `MERGE`; `dbt_build_gold` log `PASS`; `gx_validate_silver` log bảng "ĐẠT/HỎNG" của Great Expectations; `iceberg_maintenance` log "số file … → …" | DAG = phụ thuộc + thử lại + lịch chạy mỗi giờ; GX là cổng chất lượng – kỳ vọng hỏng thì gold không bị ghi đè; mỗi task là một pod trong `compute` (thấy trong k9s); dbt test đỏ thì task đỏ |
| 11–13 | Silver + gold | Grafana: *Tin trong silver* **không đổi** dù bronze có thêm 3.000 dòng; *Snapshot của silver* +1; bảng *Sức khỏe bảng Iceberg*: số file bronze giảm sau compaction; ba biểu đồ gold. Tab Trino UI: các truy vấn Grafana vừa gửi | `MERGE INTO` upsert theo `job_uid` (bản replay trùng với bản batch); chuẩn hóa lương, 34 tỉnh sau sắp xếp 2025, kinh nghiệm, cấp bậc; Trino là engine truy vấn, Grafana chỉ hiển thị |
| 13–15 | Iceberg time travel | Grafana bảng *Lịch sử snapshot*; terminal: truy vấn time travel (mục 3.3) | Mỗi commit là một phiên bản, tái lập số liệu; compaction không làm mất dữ liệu (`so_dong` giữ nguyên) |

### 3.1. Phần streaming chạy trực tiếp

Terminal B – job streaming (micro-batch 10 giây để thấy rõ trên Grafana; driver + executor 768 MB cho vừa RAM):

```bash
DRIVER_MEMORY=768m EXECUTOR_MEMORY=768m bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"
```

Đợi terminal D in `Spark UI http://localhost:4040 …`, mở tab Spark UI → *Structured Streaming*.

Terminal C – replay 3.000 tin VietJobs vào Kafka, 50 tin/giây (~1 phút). Đây là "nguồn sự kiện" (đóng vai crawler),
chạy bằng một pod Python nhỏ, không phải Spark:

```bash
bash deploy/data/replay-vietjobs.sh 3000 50
```

Xem Grafana (biểu đồ *Tin nạp vào bronze theo 10 giây*) và Kafka UI (*Topics → jobs.raw.vietjobs.v1 → Messages*).
Replay xong: `Ctrl+C` ở terminal B (chỉ dừng lệnh submit; driver vẫn chạy trong cụm), rồi chuyển sang phần
Airflow bằng `bash scripts/demo-mode.sh airflow` – lệnh này dừng hẳn job streaming và bật lại scheduler (~1 phút).

### 3.2. Phần Airflow

Airflow → DAG `joblens_pipeline` → nút **Trigger** (▶). Bấm vào ô task trong đồ thị → *Logs*. Có thể chạy bằng lệnh:

```bash
kubectl exec -n orchestration statefulset/airflow-scheduler -c scheduler -- airflow dags trigger joblens_pipeline
```

Trong lúc task Spark chạy, `k9s` (namespace `compute`) cho thấy pod driver `joblens-silver-job-postings-…` và pod
executor sinh ra rồi tự xóa khi xong.

### 3.3. Time travel

Lấy `snapshot_id` cũ từ bảng *Lịch sử snapshot* trên Grafana (hoặc truy vấn 6 trong `scripts/demo-queries.sql`):

```bash
bash scripts/trino.sh "SELECT count(*) FROM iceberg.silver.job_postings FOR VERSION AS OF <snapshot_id>"
bash scripts/trino.sh -f scripts/demo-queries.sql   # dự phòng nếu Grafana lỗi
```

## 4. Phương án dự phòng

- Mất mạng: image, plugin Grafana và dữ liệu đã nằm trong cụm, chỉ cần không xóa cụm. Không chạy `upload-vietjobs.sh`
  hay cài mới trong buổi demo.
- Grafana lỗi: chạy `bash scripts/trino.sh -f scripts/demo-queries.sql` – cùng số liệu dạng bảng.
- Job Spark lỗi giữa chừng: dùng video/ảnh chụp ở bước chuẩn bị; xem log bằng lệnh in ra cuối `submit.sh` hoặc
  log task trên Airflow (pod lỗi được giữ lại: `kubectl get pod -n compute`).
- Máy treo/hết RAM: bật video quay phần streaming và Airflow.

## 5. Sự cố hay gặp

| Triệu chứng | Cách xử lý |
|---|---|
| Ô Grafana báo *Plugin not found* | Pod Grafana lần đầu cần Internet để tải plugin Trino: `kubectl logs -n monitoring deploy/grafana \| grep -i plugin`; có mạng rồi `kubectl rollout restart -n monitoring deploy/grafana` |
| Ô Grafana báo *Table … does not exist* | Chưa chạy bước dữ liệu tương ứng (silver hoặc `dbt.sh build`) |
| Task Airflow đứng ở *queued* lâu | Scheduler đang tắt (chế độ streaming): `bash scripts/demo-mode.sh airflow` |
| Pod `Pending`, `OOMKilled`, máy chậm | Thiếu RAM: `bash scripts/demo-mode.sh status`; `kubectl describe pod -n <ns> <pod>`; dừng streaming trước khi chạy DAG |
| Task Airflow lỗi `Forbidden` khi tạo pod | Chạy lại `bash deploy/airflow/install.sh` (áp lại `deploy/airflow/rbac.yaml`) |
| Kafka UI `ImagePullBackOff` | Kiểm tra mạng; tag image ở `deploy/monitoring/kafka-ui.yaml` |
| Sau `wsl --shutdown` hoặc khởi động lại máy: `kubectl` báo *connection reset by peer* | Node k3d đổi IP, load balancer còn trỏ IP cũ: `k3d cluster stop joblens && k3d cluster start joblens`, đợi 1 phút rồi `kubectl get nodes` |
| Biểu đồ *Tin nạp vào bronze theo 10 giây* trống dù bronze tăng | Kiểm tra khoảng thời gian Grafana (Last 15/30 minutes); xem SQL thật bằng *Inspect → Query* trên ô đó |
| `demo-ui.sh` báo cổng đang dùng | Đóng port-forward cũ: `pkill -f "kubectl port-forward"` |
| Sửa DAG nhưng Airflow vẫn chạy bản cũ | Chạy lại `bash deploy/airflow/install.sh` (cập nhật ConfigMap, image, khởi động lại scheduler) |

## 6. Chưa có trong demo (nói là phần tiếp theo)

Great Expectations, đo batch vs streaming có số liệu (ADR Lambda/Kappa), dashboard giám sát hạ tầng
(Prometheus: lag Kafka, RAM pod), dedup liên trang và kỹ năng (đợi image của nhóm Web Mining), dữ liệu crawl TopCV vào
Kafka, job streaming ghép cảnh báo `alerts.job_match.v1`.
