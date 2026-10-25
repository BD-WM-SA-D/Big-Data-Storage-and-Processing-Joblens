# Chạy demo – từng bước trên màn hình (checkpoint 26/10 và demo cuối kỳ)

Tài liệu này dùng **trong buổi demo**: làm gì, bấm vào đâu, chỉ vào ô nào, nói câu gì. Tập dượt và xử lý lỗi:
[`tap-duot-demo.md`](tap-duot-demo.md). Yêu cầu môn học ↔ bằng chứng: [`yeu-cau-mon-hoc.md`](yeu-cau-mon-hoc.md).

> **Chỉ nói số đọc trên màn hình hôm đó** (quy tắc repo: không bịa số đo). Số trong ngoặc ở tài liệu này là số của
> lần chạy 23–25/10 để biết giá trị cỡ nào là bình thường.

Tổng thời gian trình bày: ~15 phút (checkpoint) – phần 5, 6 rút gọn được nếu thiếu giờ.

| Phần | Nội dung | Phút |
|---|---|---|
| A | Chuẩn bị trước giờ demo (không trình bày) | −40 → 0 |
| 1 | Bài toán + kiến trúc | 0–2 |
| 2 | Dữ liệu, lưu trữ, xử lý batch (Grafana, SeaweedFS, Trino) | 2–5 |
| 3 | Điều phối + chất lượng (Airflow, GX) | 5–7 |
| 4 | Streaming trực tiếp (Kafka, Spark, watermark) | 7–11 |
| 5 | Thêm / sửa / xóa dữ liệu tại chỗ (SQLPad, Mongo Express) | 11–14 |
| 6 | Spark nâng cao + NoSQL trên Grafana | 14–15 |
| 7 | Hỏi đáp (bảng trả lời nhanh) | sau đó |
| B | Sau buổi demo | – |

Quy ước: **T1–T4** = tab Ubuntu trong Windows Terminal (nút **˅** cạnh **+** → *Ubuntu*); mỗi tab mới chạy trước
`cd ~/joblens/Big-Data-Storage-and-Processing-joblens`. ✅ = dấu hiệu đúng. Không chiếu mật khẩu lên màn hình.

---

## A. Chuẩn bị (40 phút trước giờ demo)

### A1. Khởi động (T1) – phút −40

1. Mở **Docker Desktop**, đợi *Engine running*. Tắt bớt ứng dụng nặng trên Windows (game, nhiều tab Chrome).
2. **T1:**
   ```bash
   cd ~/joblens/Big-Data-Storage-and-Processing-joblens
   git pull
   k3d cluster start joblens
   ```
   ✅ `Started cluster 'joblens'`.
3. Đợi 3–5 phút, rồi:
   ```bash
   kubectl get pods -A | grep -v -E "Running|Completed"
   ```
   ✅ Chỉ còn dòng tiêu đề. Còn pod lỗi sau 5 phút: `k3d cluster stop joblens && k3d cluster start joblens`, đợi lại.
   Pod `Error`/`Terminating` cũ trong `compute`: `kubectl delete pod -n compute <tên>` (vô hại).
4. Máy chưa từng cài SQLPad/Mongo Express: `bash deploy/data-tools/install.sh` (✅ hai dòng `successfully rolled out`).

### A2. Mở giao diện (T1 – để nguyên đến hết buổi) – phút −33

```bash
bash scripts/demo-ui.sh
```
✅ Danh sách 8 địa chỉ và lệnh **không trả lại dấu nhắc**. Không đóng T1.

### A3. Chạy DAG một lượt cho số liệu mới (trình duyệt) – phút −32

1. Lấy mật khẩu Airflow – **T2** (mở tab mới):
   ```bash
   kubectl get secret airflow-admin -n orchestration -o jsonpath='{.data.password}' | base64 -d; echo
   ```
2. Mở http://localhost:8082 → đăng nhập `admin` → bấm `joblens_pipeline` → nút **Trigger** (▶) → **Trigger**.
3. Đợi ~5–10 phút (làm A4, A5 trong lúc đợi). ✅ 5 ô task xanh.
4. Khi đã xanh: bấm **công tắc Pause** (cạnh tên DAG) để DAG **tạm dừng** – tránh lượt chạy theo giờ chen vào giữa
   buổi demo. ✅ Công tắc chuyển sang trạng thái tạm dừng.

### A4. Mở sẵn các tab trình duyệt theo đúng thứ tự trình bày – phút −30

| Tab | Địa chỉ | Chuẩn bị |
|---|---|---|
| 1 | GitHub → repo → `docs/kien-truc.md` | Cuộn tới sơ đồ mục 2 |
| 2 | http://localhost:3000/d/joblens-demo | Góc phải trên: khoảng thời gian **Last 15 minutes**, làm mới **5s**. Muốn ẩn menu Grafana khi chiếu: thêm `&kiosk` vào cuối địa chỉ (Esc để thoát) |
| 3 | http://localhost:8888/buckets/joblens/ | SeaweedFS – trang gốc bucket |
| 4 | http://localhost:8082/dags/joblens_pipeline | Airflow, đã đăng nhập |
| 5 | http://localhost:8081/ui/clusters/joblens/all-topics | Kafka UI |
| 6 | http://localhost:3001 | SQLPad, đã đăng nhập (`admin@joblens.local`); mở sẵn **Queries** |
| 7 | http://localhost:8083/db/joblens/ | Mongo Express, đã đăng nhập (`admin`) |

Mật khẩu SQLPad / Mongo Express: `bash deploy/data-tools/install.sh` in lại (chạy ở T2, **đừng chiếu**).

### A5. Kiểm tra nhanh – phút −20

- Grafana: hàng ô số trên cùng có giá trị; các biểu đồ gold có cột; phần MongoDB phía dưới có dữ liệu. Ô *Tin nạp vào
  bronze theo 10 giây* và *Tin mới theo tỉnh – cửa sổ 5 phút* **trống là đúng** (chưa streaming – sẽ chạy ở phần 4).
- SQLPad: Queries → **7. Xem dữ liệu thật** → Run. ✅ 3 câu `finished`.
- **T2:** `bash scripts/demo-mode.sh status` – xem RAM. ✅ Không node nào gần 100%.
- Phóng to chữ terminal (`Ctrl` + `+`) và trình duyệt (`Ctrl` + `+`) cho người xem cuối phòng đọc được.

### A6. Video dự phòng

Có sẵn video quay một lượt đầy đủ trên máy (phòng khi mất mạng/hết RAM giữa buổi).

---

## 1. Bài toán + kiến trúc (phút 0–2)

1. **Tab 1 (GitHub `docs/kien-truc.md`)**. Nói:
   - *"Bài toán: thị trường tuyển dụng Việt Nam – lương theo tỉnh, cấp bậc, nhóm nghề và lượng tin mới theo thời gian.
     Dữ liệu thật là VietJobs, 48.092 tin; định dạng lương/địa điểm không thống nhất."*
   - Chỉ sơ đồ luồng từ trái sang phải: *"Kafka nhận tin → Spark Structured Streaming ghi bronze trên Iceberg → Spark
     MERGE ra silver đã chuẩn hóa → Great Expectations kiểm tra → dbt xây gold → Trino truy vấn → Grafana. Song song,
     một job streaming có trạng thái ghi cửa sổ thời gian vào MongoDB."*
   - *"Kiến trúc Kappa cho đường nạp – một mã cho cả dữ liệu sống và nạp lại lịch sử; silver/gold chạy batch theo
     lịch vì cần MERGE, test dbt và cổng chất lượng. Lý do và số đo ở ADR 0001."*
2. Cuộn xuống sơ đồ triển khai. **T2:**
   ```bash
   kubectl get pods -A
   ```
   Chỉ cột NAMESPACE: *"Mọi thứ chạy trên Kubernetes (k3d), mỗi tầng một namespace: `ingest` Kafka, `lakehouse`
   SeaweedFS + Lakekeeper, `compute` Spark, `orchestration` Airflow, `serving` Trino + MongoDB, `monitoring` Grafana.
   Toàn bộ khai báo trong thư mục `deploy/` của repo."*

## 2. Dữ liệu, lưu trữ, xử lý batch (phút 2–5)

1. **Tab 2 (Grafana)** – hàng ô số trên cùng, chỉ lần lượt từ trái sang phải:

   | Ô | Nói |
   |---|---|
   | **Tin trong silver** (~48,1K) | *"Một dòng một tin sau MERGE theo job_uid."* |
   | **Dòng trong bronze** (~75K) | *"Bronze giữ nguyên mọi bản nhận được, kể cả bản trùng do replay – nhiều hơn silver là đúng thiết kế: khử trùng ở silver, bronze là lịch sử thô để xử lý lại."* |
   | **Tỉ lệ có địa điểm** (~98,5%) | *"Chuỗi địa điểm tự do ('Hà Đông, Hà Nội'…) ánh xạ về 34 tỉnh/thành sau sắp xếp 2025."* |
   | **Tỉ lệ có lương** (~71,5%) | *"Phần còn lại là 'Thỏa thuận' – giữ lại, đánh dấu negotiable."* |
   | **Số file dữ liệu bronze** | *"Streaming sinh file nhỏ; task bảo trì của Airflow gộp lại."* |
   | **Snapshot của silver** | *"Mỗi lần job silver commit là một snapshot Iceberg – quay lại được (time travel)."* |

2. Kéo xuống hai biểu đồ gold:
   - **Số tin theo tỉnh/thành (top 15)** – *"Hà Nội và TP.HCM chiếm phần lớn – dữ liệu lệch, nên job phân tích dùng
     broadcast join cho bảng tỉnh."*
   - **Lương trung vị theo cấp bậc** – đọc 2–3 cột (ví dụ manager, mid, intern): *"Bảng gold do dbt xây, có 12 test."*
3. **Sức khỏe bảng Iceberg** (bảng bên phải): *"Số file và kích thước trung bình mỗi bảng – theo dõi vấn đề file nhỏ."*
4. **Tab 3 (SeaweedFS)**: bấm `warehouse/` → thư mục bảng `jobs_raw` → `data/` (thư mục con theo ngày và nguồn = phân
   vùng `days(ingest_ts), source`) → quay lại → `metadata/` (`*.metadata.json`). Nói: *"Lưu trữ phân tán dùng S3
   (SeaweedFS) thay HDFS; Iceberg là định dạng bảng trên đó: file Parquet + metadata, có ACID và snapshot."*
5. **Tab 6 (SQLPad)** → **Queries** → **7. Xem dữ liệu thật** → **Run** → bấm câu lệnh thứ 2 trong bảng tóm tắt.
   ✅ 10 tỉnh nhiều tin nhất với lương trung vị. Nói: *"Trino truy vấn trực tiếp bảng Iceberg bằng SQL."*

## 3. Điều phối + chất lượng (phút 5–7)

1. **Tab 4 (Airflow)** → `joblens_pipeline` → tab **Graph**. Chỉ:
   `silver_job_postings → gx_validate_silver → {dbt_build_gold, spark_analytics → iceberg_maintenance}`.
   Nói: *"Mỗi task là một pod Kubernetes. GX hỏng thì gold không được xây lại – cổng chất lượng."*
2. Bấm ô **gx_validate_silver** của lượt vừa chạy (phần A3) → **Logs** → cuộn tới bảng kết quả.
   ✅ Dòng cuối `→ 15/15 kỳ vọng đạt`. Chỉ 2–3 dòng: `job_uid` không trùng, lương trong khoảng hợp lệ, tỉ lệ có địa
   điểm ≥ 90%.
   Log không mở được: **T2** `kubectl logs -n compute $(kubectl get pods -n compute -o name | grep gx-silver | tail -1) | grep -A20 "Kết quả GX"`.
3. Tab **Gantt**: đọc thời gian từng task của lượt này.

## 4. Streaming trực tiếp (phút 7–11)

1. **T2** – chuyển cụm sang chế độ streaming:
   ```bash
   bash scripts/demo-mode.sh streaming
   ```
   ✅ `đã tạm dừng` (hoặc `scheduler đang tắt`) rồi dòng cuối `Giờ chạy: bash deploy/spark/submit.sh bronze_jobs …`.
   Nói: *"Máy 16 GB nên tạm tắt Airflow khi chạy streaming."*
2. **T2** – job streaming có trạng thái (cửa sổ 5 phút theo tỉnh → MongoDB):
   ```bash
   DRIVER_MEMORY=768m EXECUTORS=0 bash deploy/spark/submit.sh stream_province_windows
   ```
3. **T3** (tab mới, `cd` vào repo) – job streaming nạp bronze, micro-batch 10 giây:
   ```bash
   DRIVER_MEMORY=768m EXECUTOR_MEMORY=768m bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"
   ```
   Trong 1–2 phút chờ hai job khởi động: nói về exactly-once – *"offset Kafka và state lưu trong checkpoint; MongoDB
   upsert theo khóa cửa sổ × tỉnh nên chạy lại một micro-batch vẫn cho cùng kết quả."*
   ✅ T2 và T3 đều lặp lại `Streaming query has been idle and waiting for new data`.
4. **T4** (tab mới, `cd` vào repo) – **chỉ khi T2 và T3 đã `idle`**:
   ```bash
   bash deploy/data/replay-vietjobs.sh 1000 50 --late-every 10
   ```
   Nói: *"Gửi 1.000 tin, 50 tin/giây; cứ 10 tin có 1 tin giả lập đến trễ 20 phút."*
   ✅ `đã gửi 1000/1000 tin` → `==> Xong`.
5. **Tab 5 (Kafka UI)** → `jobs.raw.vietjobs.v1` → **Messages**: bấm một message – JSON `job_posting.v1`, key =
   `source_job_id`. Nói: *"Topic 6 partition, key theo tin nên cùng tin luôn vào cùng partition."*
6. **Tab 2 (Grafana)**, **Last 15 minutes**:
   - **Tin nạp vào bronze theo 10 giây** – cột mới mỗi 10 giây. *"Độ trễ đo được: streaming p95 ~60 s so với batch
     ~122 s (benchmarks/batch_vs_streaming.md)."*
   - **Dòng trong bronze** tăng thêm 1.000; **Tin trong silver** không đổi (silver chạy batch, và tin replay đã có).
   - Kéo xuống **Tin mới theo tỉnh – cửa sổ 5 phút** – cột màu theo tỉnh. *"Đếm theo thời điểm crawl (event time),
     không theo lúc Spark nhận; tên tỉnh lấy từ Iceberg – Trino join MongoDB với Iceberg trong một truy vấn."*
7. **T4** – số đo watermark:
   ```bash
   POD=$(kubectl get pods -n compute --no-headers | grep province | grep Running | awk '{print $1}')
   kubectl logs -n compute "$POD" | grep -E "tiến độ batch" | tail -6
   ```
   ✅ Có dòng `bỏ do trễ (watermark) K` với K > 0. Nói: *"Watermark = thời điểm lớn nhất đã thấy trừ 10 phút; tin trễ
   hơn bị bỏ và Spark xóa được state cửa sổ cũ. K đếm theo nhóm cửa sổ × tỉnh, không theo từng tin."*

Để T2, T3 chạy tiếp sang phần 5.

## 5. Thêm / sửa / xóa dữ liệu tại chỗ (phút 11–14)

Chọn **một** trong hai cách (trình duyệt đẹp hơn khi chiếu; terminal khi giảng viên muốn tự gõ).

### 5.1. Trên trình duyệt (khuyên dùng)

**Tab 6 (SQLPad)** → **Queries**. Mỗi câu mẫu: mở → **Run** → trong bảng tóm tắt bấm **câu lệnh cuối** để xem dữ liệu
(hoặc bôi đen riêng câu `SELECT` rồi Run để bảng hiện ngay).

| Câu mẫu | Nói | ✅ Kết quả câu lệnh cuối |
|---|---|---|
| 1. tạo bảng | *"Tạo bảng Iceberg mới, phân vùng theo cấp bậc."* | bảng rỗng có 6 cột |
| 2. INSERT | *"Thêm một tin."* (có thể sửa tiêu đề/lương theo yêu cầu của thầy trước khi Run) | dòng `web-1`, 20000000–30000000 |
| 3. UPDATE | *"Iceberg v2 ghi delete file + file mới, không sửa đè file cũ."* | `salary_max_vnd` = 35000000 |
| 4. DELETE | *"Xóa theo dòng."* | `so_dong_con_lai` = 0 |
| 5. snapshot | *"Mỗi thao tác là một snapshot; chép snapshot_id để quay lại."* | các dòng `append`, `overwrite`/`delete` |
| 6. MongoDB qua Trino | *"Trino tạo collection MongoDB, ghi rồi join với bảng tỉnh trong Iceberg."* | `n1` Hà Nội, `n2` TP.HCM |

Time travel (sau câu 5): bấm **New**, gõ (thay `<id>` bằng `snapshot_id` **dòng đầu tiên** của câu 5 – snapshot của
lần INSERT) rồi Run:
```sql
SELECT * FROM iceberg.sandbox.web_jobs FOR VERSION AS OF <id>
```
✅ Dòng `web-1` với `salary_max_vnd` = **30000000** – dữ liệu trước UPDATE và DELETE, dù bảng hiện tại đã rỗng.

**Tab 7 (Mongo Express)** → tải lại trang → **web_notes** → **New Document** →
`{ "note_id": "n3", "location_code": "48", "content": "Thêm từ Mongo Express" }` → **Save**; bấm `n3` sửa `content`
→ **Save**. Quay lại SQLPad câu 6, bôi đen câu `SELECT` cuối → Run. ✅ Có `n3` – *Thành phố Đà Nẵng*. Nói: *"Sửa
document trên MongoDB, Trino đọc ngay."*

### 5.2. Trong terminal (T4)

- Theo kịch bản: `bash scripts/demo-data.sh sql` (Enter qua từng bước), `bash scripts/demo-data.sh nosql`.
- Tự gõ: `bash scripts/trino.sh` (dấu nhắc `trino>`, thoát `quit`), `bash scripts/mongo.sh` (thoát `exit`) – ví dụ ở
  [`tap-duot-demo.md`](tap-duot-demo.md) mục 5b.

### 5.3. Một tin mới đi hết pipeline (nếu còn giờ, ~3–5 phút, T4)

```bash
bash scripts/demo-data.sh kafka "Kỹ sư dữ liệu (tin demo)" "15 - 25 triệu" "Đà Nẵng"
```
(Có thể để thầy chọn tiêu đề, mức lương, tỉnh.) Bấm Enter qua 3 bước. ✅ Bronze có dòng `synthetic | demo-…`; silver có
`15000000 | 25000000 | [48] | mid`. Nói: *"Tin vào Kafka, job streaming đang chạy ghi bronze, job silver chuẩn hóa
chuỗi '15 - 25 triệu' thành số VND và 'Đà Nẵng' thành mã tỉnh 48."*

## 6. Spark nâng cao + NoSQL trên Grafana (phút 14–15)

**Tab 2 (Grafana)**, kéo xuống phần **NoSQL (MongoDB) + Spark nâng cao**:

| Ô | Nói |
|---|---|
| **Lương theo tỉnh: trung vị vs trung bình cắt 10%** | *"Hai cột: hàm có sẵn percentile_approx và hàm tổng hợp tự viết (pandas UDF trimmed_mean) – kết quả Spark ghi vào MongoDB."* |
| **Pivot vùng × cấp bậc** | *"Spark pivot; có kiểm tra unpivot cộng lại khớp tổng."* |
| **Lần chạy spark_analytics gần nhất** | *"Mô hình lương MLlib với CrossValidator: RMSE (~10,3 triệu) thấp hơn mô hình luôn đoán trung vị (~11,2). Cột plan_checks: kiểm tra tự động kế hoạch thực thi – có BroadcastHashJoin, SortMergeJoin, bộ lọc đẩy xuống bronze (partition pruning), pivot đủ cấp bậc."* |

(Kế hoạch thực thi đầy đủ: Tab 3 SeaweedFS → `reports/spark-plans/` → mở `broadcast_join.txt`.)

Kết: *"Repo có mã chạy được (README – Chạy nhanh, 179 kiểm thử), dữ liệu thật trên lakehouse, kiến trúc và quyết định
ghi trong docs/. Việc tiếp: đo lại batch vs streaming, đo compaction, ghép crawler và dedup của nhóm Web Mining."*

## 7. Hỏi đáp – trả lời nhanh

| Câu hỏi | Trả lời + chỉ vào đâu |
|---|---|
| Xử lý trùng thế nào? | Bronze giữ mọi bản; silver `MERGE` theo `job_uid = sha1(source:source_job_id)` → Grafana bronze > silver; GX kiểm `job_uid` không trùng. Trùng giữa các trang (gần giống) là phần Web Mining (`silver.job_dedup`, ADR 0001 D5) |
| Vì sao Kappa, không Lambda? | Một mã nạp cho dữ liệu sống và lịch sử; silver/gold cần MERGE + dbt + GX nên chạy batch; số đo p95 streaming ~60 s vs batch ~122 s, hai đường cho cùng tập khóa – `docs/adr/0001-lambda-vs-kappa.md`, `benchmarks/batch_vs_streaming.md` |
| Vì sao không dùng HDFS? | S3 API (SeaweedFS) là chuẩn lưu trữ đối tượng phổ biến, nhẹ cho cụm một máy; Iceberg chạy trên S3 nên có ACID, snapshot, schema evolution mà HDFS thuần không có |
| Dữ liệu đến trễ? | Event time `crawl_ts` + watermark 10 phút (phần 4, bước 7) |
| Exactly-once? | Checkpoint (offset Kafka + state) + commit Iceberg; MongoDB upsert idempotent theo `_id` cửa sổ × tỉnh |
| Dữ liệu lớn cỡ nào? | 48.092 tin VietJobs (~75K dòng bronze kể cả replay); thiết kế phân vùng/bucket để mở rộng – partition `days(ingest_ts), source`, `bucket(16, job_uid)` |
| Chạy lại từ đầu được không? | `bash scripts/bootstrap-all.sh` trên máy trống (~30–40 phút) hoặc GitHub Codespaces |
| Chất lượng dữ liệu? | GX 15 kỳ vọng làm cổng trước gold; dbt 12 test trên gold |

## B. Sau buổi demo

1. **T4** – trả cụm về chế độ thường (dừng streaming, bật Airflow, bỏ tạm dừng DAG):
   ```bash
   bash scripts/demo-mode.sh airflow
   ```
   ✅ `==> Đã bỏ tạm dừng DAG joblens_pipeline`. Đóng T2, T3.
2. Dọn dữ liệu thử: SQLPad → **8. Dọn dẹp dữ liệu thử** → Run; **T4** `bash scripts/demo-data.sh clean`.
3. Ghi một dòng vào `deploy/compat.md`: ngày, commit (`git log --oneline -1`), số đo đọc được hôm đó (bronze/silver,
   watermark K, thời gian DAG), `kiểm tra: <người kiểm tra>`; commit và push.
4. **T1**: Ctrl+C; `k3d cluster stop joblens`.

## Khi có sự cố giữa buổi

| Sự cố | Làm ngay |
|---|---|
| Grafana trắng / *Unable to find application file* | Tải lại trang (F5); vẫn lỗi → SQLPad câu 7 hoặc `bash scripts/trino.sh -f scripts/demo-queries.sql` ở T2 |
| Một ô Grafana đỏ | Di chuột vào ⚠ đọc lỗi; nói tiếp phần khác, xem `tap-duot-demo.md` sau |
| `demo-mode.sh streaming` đợi task Airflow lâu | Ctrl+C, vào Airflow bấm Pause DAG, chạy lại lệnh |
| Job streaming không lên sau 3 phút | Bỏ phần 4, chiếu video dự phòng đoạn streaming |
| Mất kết nối localhost (port-forward rơi) | T1: Ctrl+C rồi `bash scripts/demo-ui.sh` |
| Hết RAM, máy treo | Chuyển sang video dự phòng |
