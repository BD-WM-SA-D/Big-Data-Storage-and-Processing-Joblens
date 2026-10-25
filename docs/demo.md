# Kịch bản demo môn Big Data (IT4931) – nền tảng JobLens

Mục tiêu: trong ~18 phút cho người xem **thấy tận mắt** dữ liệu được **di chuyển, lưu trữ, xử lý** thế nào trên nền
tảng tự dựng, và **mỗi yêu cầu của bài tập lớn** được đáp ứng ở đâu. Yêu cầu lấy từ trang môn học
(<https://datalab.vn/teaching/it4931-luu-tru-va-xu-ly-du-lieu-lon>); bảng đối chiếu đầy đủ ở
[`docs/yeu-cau-mon-hoc.md`](yeu-cau-mon-hoc.md). Dữ liệu thật: VietJobs 48.092 tin. Phần lớn buổi demo diễn ra trên
**trình duyệt**; terminal chỉ dùng để bấm chạy job.

> Số liệu nói trong buổi demo phải là số đo thật từ lần chạy hôm đó (quy tắc repo: không bịa số đo).
> Ghi lại vào `deploy/compat.md` sau khi chạy.

## Checkpoint 26/10 – yêu cầu và cách trình bày (~10 phút)

Yêu cầu giảng viên: *git có mã chạy được, dữ liệu và kiến trúc hệ thống đã sẵn sàng*. Tập dượt từng bước (terminal nào, đầu ra nào là thành công):
[`tap-duot-demo.md`](tap-duot-demo.md).

| Yêu cầu | Cho xem gì |
|---|---|
| Git có mã chạy được | GitHub repo: `README.md` (mục *Chạy nhanh*), lịch sử commit; terminal: `uv run pytest -q` (hoặc kết quả chạy trước), `kubectl get pods -A` – hệ thống đang chạy từ chính mã trong repo |
| Kiến trúc | [`kien-truc.md`](kien-truc.md) mục 2–3 (sơ đồ luồng + triển khai, GitHub vẽ sẵn Mermaid); ADR Lambda/Kappa; `k9s` hoặc `kubectl get pods -A` theo namespace |
| Dữ liệu | [`kien-truc.md`](kien-truc.md) mục 4 (nguồn, các tầng, số liệu); Grafana (số dòng bronze/silver, tỉ lệ chuẩn hóa); SeaweedFS (file Parquet + metadata Iceberg); `bash scripts/trino.sh -f scripts/demo-queries.sql` |
| Nếu được hỏi "thêm/sửa/xóa dữ liệu thử" | `bash scripts/demo-data.sh sql` (Iceberg), `nosql` (MongoDB), `kafka "<tiêu đề>" "<lương>" "<tỉnh>"` (một tin đi hết pipeline) – mục 3.4 |

Thứ tự gợi ý: kiến trúc (2 phút) → dữ liệu trên Grafana + Trino (3 phút) → streaming ngắn: replay 1.000 tin, ô bronze
và cửa sổ tỉnh tăng (3 phút) → DAG Airflow đã chạy xanh (1 phút) → việc còn lại (1 phút). Chạy thử
`demo-data.sh sql`, `nosql`, `kafka` một lần trước buổi (rồi `clean`) để chắc chạy được trên máy mình.

## 0. Giao diện dùng trong buổi demo

Tất cả mở bằng một lệnh `bash scripts/demo-ui.sh` (port-forward ra `localhost`; trên Codespaces mở ở tab PORTS):

| Giao diện | Địa chỉ | Cho thấy gì | Yêu cầu môn học |
|---|---|---|---|
| **Grafana** – "JobLens – Big Data demo" | http://localhost:3000 | Bronze/silver, tốc độ nạp, gold, snapshot/số file Iceberg; phần dưới: cửa sổ streaming và kết quả Spark trong **MongoDB** (qua Trino, có join MongoDB × Iceberg) | Trực quan hóa, NoSQL |
| **Kafka UI** (chỉ đọc) | http://localhost:8081 | Topic, 6 partition, message JSON | Hàng đợi tin nhắn |
| **Spark UI** (`bash scripts/demo-ui.sh spark`) | http://localhost:4040 | *Structured Streaming*: input rate, batch duration, state; *SQL*: kế hoạch thực thi | Spark streaming, tối ưu |
| **Airflow** | http://localhost:8082 | DAG `joblens_pipeline`: silver → GX → dbt + Spark analytics → bảo trì | Pipeline end-to-end |
| **Trino UI** | http://localhost:8080 | Truy vấn liên nguồn Iceberg + MongoDB | Truy vấn |
| **SeaweedFS** | http://localhost:8888/buckets/joblens/ | `warehouse/` (Parquet + metadata Iceberg), `reports/spark-plans/` (kế hoạch thực thi), `quality/gx/` | Lưu trữ phân tán |
| k9s (terminal) | `k9s` | Pod của từng tầng; pod executor sinh ra rồi mất | Triển khai Kubernetes |

Đăng nhập: Grafana xem không cần đăng nhập; Airflow `admin` + mật khẩu do `deploy/airflow/install.sh` in ra;
Trino nhập user bất kỳ.

## 1. Cài đặt

**Máy cá nhân** (đã có pipeline theo `deploy/README.md`): thêm MongoDB và các job mới:

```bash
cd ~/joblens/Big-Data-Storage-and-Processing-joblens
git pull && uv sync
bash deploy/mongodb/install.sh       # MongoDB 8.0 + user ứng dụng + Secret joblens-mongo
bash deploy/trino/install.sh         # thêm catalog mongodb
bash deploy/airflow/install.sh       # build lại image (pymongo, pyarrow) + DAG có task spark_analytics
bash deploy/monitoring/install.sh    # dashboard có phần MongoDB
```

**Máy mới / cloud:** `bash scripts/bootstrap-all.sh` dựng tất cả từ đầu (~30–40 phút). Chạy trên GitHub Codespaces
(miễn phí, không cần thẻ): [`docs/cloud-codespaces.md`](cloud-codespaces.md).

**RAM.** Máy 16 GB: WSL `memory=12GB`, `swap=8GB`; máy 8 GB: `JOBLENS_PROFILE=lite` (xem `deploy/README.md`). Trong buổi
demo **không chạy streaming và Airflow cùng lúc** – `scripts/demo-mode.sh` chuyển chế độ (tạm dừng DAG, đợi task đang
chạy xong rồi mới tắt scheduler). Xem RAM: `bash scripts/demo-mode.sh status`.

## 2. Chuẩn bị (trước buổi demo ít nhất 1 ngày)

1. Dữ liệu đã có trong bronze/silver/gold: `bash scripts/trino.sh -f scripts/demo-queries.sql`.
2. Chạy DAG một lần (Airflow → `joblens_pipeline` → **Trigger**), cả 5 task xanh. Tab *Gantt*: ghi thời gian từng task.
   Sau lần này MongoDB có `analytics_*`, SeaweedFS có `reports/spark-plans/`.
3. Chạy thử phần streaming (mục 3.1) một lần để MongoDB có `province_windows`.
4. Mở Grafana, kiểm tra mọi ô có số. Ô nào báo lỗi → mục 5.
5. Tập một lượt mục 3, **quay màn hình** làm phương án dự phòng.
6. Slide số liệu: `benchmarks/batch_vs_streaming.md` (độ trễ, số file, RAM) và bảng GX 15/15.

## 3. Kịch bản trình bày

Trước giờ demo 15 phút:

- Terminal A: `bash scripts/demo-ui.sh` (để yên). Terminal D: `bash scripts/demo-ui.sh spark`.
- Trình duyệt mở sẵn: Grafana → Kafka UI → Spark UI → Airflow → Trino UI → SeaweedFS. Grafana: **Last 15 minutes**, **5s**.
- `bash scripts/demo-mode.sh streaming` (tạm dừng DAG, tắt scheduler để có RAM cho streaming).

| Phút | Phần | Làm gì trên màn hình | Yêu cầu / ý chính |
|---|---|---|---|
| 0–2 | Bài toán + kiến trúc | Slide bài toán (tin tuyển dụng VN, quy mô, giới hạn); sơ đồ luồng; `k9s` `:ns` – `ingest`, `lakehouse`, `compute`, `serving` (Trino, MongoDB), `orchestration`, `monitoring` | Định nghĩa bài toán; kiến trúc; triển khai **Kubernetes** |
| 2–4 | Lưu trữ + batch | Grafana: *Dòng trong bronze*, *Tin trong silver*. SeaweedFS: `warehouse/…/jobs_raw/data/` theo phân vùng, `metadata/*.metadata.json` | **Lưu trữ phân tán** (S3 thay HDFS) + Iceberg; partition `days(ingest_ts), source` |
| 4–9 | Streaming – mục 3.1 | Hai job streaming + replay có tin trễ. Kafka UI: message tăng. Spark UI: *Structured Streaming*. Grafana: cột bronze mọc mỗi 10 giây; ô *Tin mới theo tỉnh – cửa sổ 5 phút* (MongoDB) tăng. Log job cửa sổ: "bỏ do trễ (watermark) N" | **Kafka**; **Structured Streaming**: output mode update, **watermark** + dữ liệu trễ, **state**, **exactly-once** (checkpoint + upsert idempotent); **NoSQL** làm serving |
| 9–13 | Điều phối + chất lượng + Spark nâng cao – mục 3.2 | `demo-mode.sh airflow` → Trigger DAG. Log `gx_validate_silver`: bảng 15 kỳ vọng. Log `spark_analytics`: "kế hoạch thực thi – broadcast_join: ĐÚNG…", chỉ số mô hình. SeaweedFS: `reports/spark-plans/…/broadcast_join.txt` (BroadcastHashJoin), `sort_merge_join.txt` | Pipeline end-to-end; chất lượng dữ liệu; **Spark**: pivot, hàm tổng hợp tùy biến, broadcast vs sort-merge join, partition pruning, cache, phân tích plan, **MLlib** |
| 13–15 | Kết quả | Grafana: silver không đổi dù bronze tăng (MERGE theo job_uid); biểu đồ gold; phần MongoDB: trung vị vs trung bình cắt 10%, pivot vùng × cấp bậc, bảng mô hình (RMSE so với đoán trung vị) | Trực quan hóa; truy vấn liên nguồn Trino (Iceberg × MongoDB) |
| 15–18 | Lambda hay Kappa + bài học | Slide số đo `benchmarks/batch_vs_streaming.md`; Grafana: snapshot Iceberg, time travel (mục 3.3); 2–3 bài học tiêu biểu từ `docs/report/bai-hoc-kinh-nghiem.md` | **Lambda/Kappa** có số đo; bài học kinh nghiệm |

### 3.1. Phần streaming chạy trực tiếp

Terminal B – job streaming nạp bronze (micro-batch 10 giây):

```bash
DRIVER_MEMORY=768m EXECUTOR_MEMORY=768m bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"
```

Terminal E – job streaming **có trạng thái**: đếm tin theo tỉnh trong cửa sổ 5 phút, watermark 10 phút → MongoDB.
Chạy local mode (một pod) cho đỡ RAM:

```bash
EXECUTORS=0 bash deploy/spark/submit.sh stream_province_windows --window "5 minutes" --watermark "10 minutes"
```

Terminal C – replay 3.000 tin, 50 tin/giây; cứ 10 tin có 1 tin "đến trễ" 20 phút (crawl_ts lùi lại):

```bash
bash deploy/data/replay-vietjobs.sh 3000 50 --late-every 10
```

Chỉ ra: Kafka UI (*Topics → jobs.raw.vietjobs.v1 → Messages*); Grafana hai ô streaming; log terminal E dòng
`tiến độ batch …: … bỏ do trễ (watermark) N; watermark …` – tin trễ hơn watermark không được cộng vào cửa sổ đã đóng.
N đếm theo dòng đã gộp một phần (cửa sổ × tỉnh), không theo từng tin, nên nhỏ hơn số tin trễ đã gửi.
Đổi `--output-mode append` (chạy lần khác) để thấy cửa sổ chỉ được ghi một lần khi đã đóng.

Replay xong: Ctrl+C ở terminal B và E (chỉ dừng xem log), rồi `bash scripts/demo-mode.sh airflow` – dừng hẳn cả hai
job streaming và bật lại Airflow.

### 3.2. Phần Airflow

Airflow → `joblens_pipeline` → **Trigger** (▶). Đồ thị:
`silver_job_postings → gx_validate_silver → {dbt_build_gold, spark_analytics → iceberg_maintenance}`.
Bấm ô task → *Logs*. Nếu giao diện không đọc được log (scheduler bận), xem thẳng pod:
`kubectl logs -n compute $(kubectl get pods -n compute -o name | grep spark-analytics | tail -1) | grep -E "kế hoạch|model|rmse"`.

### 3.3. Time travel

```bash
bash scripts/trino.sh "SELECT count(*) FROM iceberg.silver.job_postings FOR VERSION AS OF <snapshot_id>"
bash scripts/trino.sh -f scripts/demo-queries.sql   # dự phòng nếu Grafana lỗi
```

### 3.4. Thao tác dữ liệu tại chỗ (khi được hỏi)

`scripts/demo-data.sh` in từng câu lệnh rồi dừng chờ Enter để giải thích. Chỉ ghi vào chỗ dành cho demo
(namespace Iceberg `sandbox`, collection `sandbox_jobs`, tin `source = synthetic` có mã `demo-…`).

```bash
bash scripts/demo-data.sh sql      # CTAS → INSERT → UPDATE → DELETE → snapshot → time travel (Trino trên Iceberg)
bash scripts/demo-data.sh nosql    # insertMany → find → updateOne → deleteOne → aggregate → Trino join MongoDB × Iceberg
bash scripts/demo-data.sh kafka "Kỹ sư dữ liệu (tin demo)" "15 - 25 triệu" "Đà Nẵng"
                                   # Kafka → bronze → silver: thấy lương, mã tỉnh, cấp bậc được chuẩn hóa (~3–5 phút)
bash scripts/demo-data.sh clean    # dọn sau buổi demo
```

Lệnh `kafka` tự chạy `bronze_jobs --once` nếu job streaming không chạy, rồi chạy `silver_job_postings` – chạy ở chế độ
Airflow (`demo-mode.sh airflow`) cũng được vì không có job streaming nào chạy cùng. Tin demo không vào gold (dbt bỏ
tin synthetic).

## 4. Phương án dự phòng

- Mất mạng: image, plugin Grafana và dữ liệu đã nằm trong cụm. Không cài mới trong buổi demo.
- Máy cá nhân hỏng/hết RAM: codespace đã dựng sẵn (`docs/cloud-codespaces.md`), hoặc video quay hôm trước.
- Grafana lỗi: `bash scripts/trino.sh -f scripts/demo-queries.sql`.
- Job Spark lỗi: log ở Airflow hoặc `kubectl logs` pod (pod lỗi được giữ lại).

## 5. Sự cố hay gặp

| Triệu chứng | Cách xử lý |
|---|---|
| Ô Grafana báo *Plugin not found* | Pod Grafana lần đầu cần Internet để tải plugin Trino: `kubectl logs -n monitoring deploy/grafana \| grep -i plugin`; có mạng rồi `kubectl rollout restart -n monitoring deploy/grafana` |
| Ô Grafana báo *Table … does not exist* | Chưa chạy bước dữ liệu tương ứng (silver, `dbt.sh build`, DAG cho `analytics_*`, job cửa sổ cho `province_windows`) |
| Ô cửa sổ 5 phút báo *Column 'window_start' cannot be resolved* | Trino đã lưu lược đồ đoán từ collection rỗng. Job `stream_province_windows` tự ghi lại lược đồ khi khởi động – chạy lại job; hoặc xóa mục `province_windows` trong collection `_schema` của MongoDB (`db.getCollection("_schema")`) |
| Ô MongoDB báo *Catalog 'mongodb' does not exist* | Chưa chạy `deploy/mongodb/install.sh` rồi `deploy/trino/install.sh` |
| Task Airflow đứng ở *queued* lâu | DAG đang tạm dừng/scheduler tắt (chế độ streaming): `bash scripts/demo-mode.sh airflow` |
| Pod `Pending`, `OOMKilled`, máy chậm | Thiếu RAM: `bash scripts/demo-mode.sh status`; dừng streaming trước khi chạy DAG |
| Task Airflow lỗi `Forbidden` khi tạo pod | Chạy lại `bash deploy/airflow/install.sh` |
| Sau `wsl --shutdown`, khởi động lại máy hoặc codespace: `kubectl` báo *connection reset* / *refused* | Bật Docker; `k3d cluster stop joblens && k3d cluster start joblens`, đợi 1 phút rồi `kubectl get nodes` |
| Kafka `CrashLoopBackOff` sau khi tắt máy đột ngột | Đợi 3–5 phút (tự hồi phục); không xóa PVC. Lần sau dừng cụm trước khi tắt máy |
| `demo-ui.sh` báo cổng đang dùng | `pkill -f "kubectl port-forward"` |
| Sửa DAG nhưng Airflow vẫn chạy bản cũ | Chạy lại `bash deploy/airflow/install.sh` |

## 6. Chưa có (nói là phần tiếp theo)

GraphFrames (đồ thị kỹ năng – bài của nhóm Web Mining), giám sát hạ tầng bằng Prometheus (lag Kafka, RAM pod), dedup
liên trang và kỹ năng (đợi image Web Mining), dữ liệu crawl TopCV vào Kafka, job ghép cảnh báo `alerts.job_match.v1`.
