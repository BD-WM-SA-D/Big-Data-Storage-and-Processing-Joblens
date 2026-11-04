# Yêu cầu bài tập lớn IT4931 và đối chiếu với nền tảng JobLens

Nguồn: trang môn học của giảng viên – <https://datalab.vn/teaching/it4043e-big-data-storage-and-processing> (bản tiếng
Anh IT4043E; bản tiếng Việt <https://datalab.vn/teaching/it4931-luu-tru-va-xu-ly-du-lieu-lon> cùng nội dung), mục
"Hướng dẫn bài tập lớn" (*milestone project*). Đọc lại ngày **2026-11-03**: nội dung không đổi so với lần đọc
2026-10-23. Trang **không** ghi thang điểm, hạn nộp, yêu cầu buổi demo hay thông báo mới – hỏi lại giảng viên.

Dùng tài liệu này để: (1) biết chính xác giảng viên yêu cầu gì, (2) biết mỗi yêu cầu được chứng minh ở file nào và
bước demo nào, (3) biết việc còn lại (mục 7). Phần phối hợp với repo Web Mining: [`phoi-hop-web-mining.md`](phoi-hop-web-mining.md).

Trạng thái (cập nhật 2026-11-03): ✅ có và đã chạy trên cụm · 🧪 có mã + kiểm thử, chưa chạy đủ trên cụm hoặc chưa đo ·
⬜ chưa có

---

## 1. Yêu cầu trên trang môn học (tóm tắt sát nguyên văn)

**Mục tiêu**
- Xây dựng một hệ thống xử lý dữ liệu lớn **hoàn chỉnh**, giải quyết một **bài toán thực tế**.
- Áp dụng kiến trúc **Lambda hoặc Kappa**.
- Pipeline đầu-cuối: **thu thập → xử lý → lưu trữ → trực quan hóa**.

**Công nghệ bắt buộc**

| Thành phần | Yêu cầu | Ghi chú của giảng viên |
|---|---|---|
| Xử lý | Apache Spark (PySpark hoặc Scala) | Dùng framework khác thì phải giải thích kiến trúc và so sánh ưu/nhược với Spark |
| Lưu trữ phân tán | HDFS hoặc tương đương | |
| Hàng đợi thông điệp | Apache Kafka, RabbitMQ hoặc tương tự | |
| Cơ sở dữ liệu | NoSQL | |
| Triển khai | Kubernetes hoặc Cloud | "Chỉ dùng Docker thì không được khuyến khích" |

**Kỹ năng Spark (mức trung cấp, qua nhiều loại transformation và action)**
1. Tổng hợp phức tạp: window function, hàm tổng hợp nâng cao, pivot/unpivot, hàm tổng hợp tùy biến.
2. Biến đổi nâng cao: biến đổi nhiều giai đoạn, chuỗi thao tác, UDF cho logic nghiệp vụ.
3. Join: broadcast join cho dữ liệu lệch, sort-merge join cho dữ liệu lớn, tối ưu nhiều join.
4. Tối ưu hiệu năng: partition pruning, bucketing, cache/persist, tối ưu truy vấn và kế hoạch thực thi.
5. Streaming: Structured Streaming với nhiều output mode, watermark và dữ liệu trễ, quản lý state, exactly-once.
6. Phân tích nâng cao: Spark MLlib, GraphFrames, tính toán thống kê, phân tích chuỗi thời gian.

**Báo cáo – 4 phần**
1. **Định nghĩa bài toán:** bài toán đã chọn, **vì sao phù hợp với dữ liệu lớn**, **phạm vi và giới hạn**.
2. **Kiến trúc và thiết kế:** thiết kế tổng thể Lambda/Kappa, thành phần và vai trò, sơ đồ luồng dữ liệu và tương tác.
3. **Chi tiết triển khai:** mã nguồn có tài liệu, cấu hình theo môi trường, triển khai và giám sát.
4. **Bài học kinh nghiệm:** mỗi bài học theo 4 phần – *Mô tả vấn đề*, *Các cách đã thử*, *Giải pháp cuối cùng*,
   *Điều rút ra* – trong 11 nhóm (mục 5).

**Lịch môn học (16 tuần, trang không ghi ngày cụ thể)**

| Tuần | Nội dung |
|---|---|
| 1–2 | Tổng quan; hệ sinh thái Hadoop |
| 3 | Kiến trúc dữ liệu lớn + giới thiệu bài tập lớn |
| 4 | HDFS |
| 5–7 | Lưu trữ NoSQL (3 buổi) |
| 8 | Hệ thống thông điệp phân tán |
| 9–10 | Spark (2 buổi) |
| 11 | Spark Structured Streaming |
| 12 | Phân tích dữ liệu lớn |
| **13–15** | **Thuyết trình bài tập lớn** |
| 16 | Tổng kết |

**Yêu cầu thêm của giảng viên sau checkpoint 26/10 (nói trực tiếp, không có trên trang):** thực hiện thêm biến đổi dữ
liệu, **thêm nhiễu vào dữ liệu rồi xử lý nhiễu** – kế hoạch ở mục 6.

---

## 2. Đối chiếu: yêu cầu chung và công nghệ

| Yêu cầu | Trạng thái | Ở đâu | Demo ([`chay-demo.md`](chay-demo.md)) |
|---|---|---|---|
| Hệ thống hoàn chỉnh, bài toán thực tế | ✅ | Tin tuyển dụng VN (VietJobs 48.092 tin) → lương / cấp bậc / tỉnh – [`kien-truc.md`](kien-truc.md) mục 1 | phần 1 |
| Kiến trúc Lambda **hoặc** Kappa | ✅ dựng xong · ✅ đủ số đo, ADR chờ nhóm duyệt | Kappa cho đường nạp + batch theo lịch cho silver/gold – [`adr/0001-lambda-vs-kappa.md`](adr/0001-lambda-vs-kappa.md), [`../benchmarks/batch_vs_streaming.md`](../benchmarks/batch_vs_streaming.md) | phần 1, 4 |
| Pipeline thu thập → xử lý → lưu trữ → trực quan hóa | ✅ | Kafka → Spark → Iceberg/MongoDB → Trino → Grafana | toàn bộ |
| Spark | ✅ | `spark-jobs/` (Spark 4.1.3, PySpark) | phần 2–6 |
| Lưu trữ phân tán (HDFS hoặc tương đương) | ✅ | SeaweedFS (S3 API) + Apache Iceberg – `deploy/storage/`. Lý do không dùng HDFS: [`kien-truc.md`](kien-truc.md), hỏi đáp ở [`chay-demo.md`](chay-demo.md) mục 7 | phần 2 |
| Hàng đợi thông điệp | ✅ | Strimzi Kafka 4.3.1 KRaft, topic khai báo bằng `KafkaTopic` – `deploy/kafka/` | phần 4 |
| NoSQL | ✅ | MongoDB 8.0 – `deploy/mongodb/`: `province_windows` (streaming), `analytics_*` (Spark) | phần 4, 6 |
| Kubernetes hoặc Cloud | ✅ k3d · 🧪 Codespaces | k3d trên máy (`deploy/`); GitHub Codespaces – [`cloud-codespaces.md`](cloud-codespaces.md) (chờ test lại sau sửa issue #1) | phần 1 |

## 3. Đối chiếu: kỹ năng Spark

| Nhóm | Kỹ thuật | Trạng thái | Ở đâu |
|---|---|---|---|
| 1. Tổng hợp phức tạp | Window function | ✅ | `silver_job_postings.py` (bản crawl mới nhất mỗi `job_uid`); `analytics_spark.py` (dense_rank trong vùng, tỉ trọng cộng dồn, trung bình trượt 3 giờ) |
| | Pivot / unpivot | ✅ | `analytics_spark.level_region_pivot` (vùng × cấp bậc, unpivot đối chiếu tổng) |
| | Hàm tổng hợp tùy biến | ✅ | `analytics_spark.trimmed_mean` (pandas UDF tổng hợp) |
| 2. Biến đổi nâng cao | Nhiều giai đoạn, chuỗi thao tác | ✅ | Kafka → parse → chuẩn hóa → MERGE (`bronze_jobs.py`, `silver_job_postings.py`) |
| | UDF cho logic nghiệp vụ | ✅ | `normalize_udf` (lương, 34 tỉnh, kinh nghiệm, cấp bậc) |
| | *Biến đổi + xử lý nhiễu (yêu cầu thêm)* | ✅ | `normalize.clean_text`, lương năm, tỉnh gõ sai, ngoại lai; `bronze.ingest_errors` – [`thi-nghiem-nhieu.md`](thi-nghiem-nhieu.md) |
| 3. Join | Broadcast join (dữ liệu lệch) | ✅ | `analytics_spark.explode_locations` – tin ⋈ 34 tỉnh; Hà Nội/TP.HCM chiếm phần lớn |
| | Sort-merge join | ✅ | `analytics_spark.activity_join` – silver ⋈ snapshot theo `job_uid` (hint MERGE) |
| | Tối ưu nhiều join | 🧪 | `bench_joins.py`: 4 cấu hình (sort-merge, broadcast, AQE, AQE + viết lại thứ tự join) – **chờ chạy trên cụm** ([`thi-nghiem-spark.md`](thi-nghiem-spark.md) mục E) |
| 4. Hiệu năng | Partition pruning | ✅ | bronze `days(ingest_ts), source`; cờ `bronze_filter_pushed` trong `plan_checks` |
| | Bucketing | ✅ | silver `bucket(16, job_uid)` |
| | Cache / persist | ✅ | `bronze_jobs.py` (persist mỗi micro-batch); `analytics_spark.py` |
| | Kế hoạch thực thi | ✅ | `analytics_spark.plan_text` → `s3://joblens/reports/spark-plans/`; cột `plan_checks` trên Grafana |
| 5. Streaming | Structured Streaming | ✅ | `bronze_jobs.py`, `stream_province_windows.py` |
| | Nhiều output mode | ✅ | `stream_province_windows.py --output-mode update\|append` – cả hai đã chạy trên cụm; `append` ghi mỗi cửa sổ đúng một lần khi đóng ([`thi-nghiem-spark.md`](thi-nghiem-spark.md) mục B) |
| | Watermark, dữ liệu trễ | ✅ | watermark 10 phút, replay `--late-every 10`: lượt 24/10 có batch bỏ 8, 4, 7 dòng (`deploy/compat.md`); lượt tập dượt 25/10 bỏ 10 dòng (chưa ghi `compat.md`) |
| | Quản lý state | ✅ | aggregation theo cửa sổ; `numRowsTotal` 30–57 dòng |
| | Exactly-once | ✅ | checkpoint offset Kafka + commit Iceberg; upsert idempotent theo `_id` (MongoDB) |
| 6. Phân tích nâng cao | Spark MLlib | ✅ | `analytics_spark.salary_model` – Pipeline + CrossValidator; RMSE 10,32 so với 11,20 của mô hình đoán trung vị ([`../benchmarks/spark_analytics.md`](../benchmarks/spark_analytics.md)) |
| | Thống kê | ✅ | tương quan kinh nghiệm–lương, phân vị |
| | Chuỗi thời gian | ✅ | lượng nạp theo giờ, trung bình trượt (`ingest_hourly`) |
| | GraphFrames | ⬜ | chưa làm – cần hỏi giảng viên; tránh trùng bài đồ thị kỹ năng của Web Mining (WM6) – xem [`phoi-hop-web-mining.md`](phoi-hop-web-mining.md) mục 6 |

## 4. Đối chiếu: báo cáo

| Phần | Giảng viên yêu cầu | Trạng thái | Nguồn trong repo |
|---|---|---|---|
| 1. Định nghĩa bài toán | bài toán; vì sao hợp dữ liệu lớn; phạm vi, giới hạn | ⬜ | [`kien-truc.md`](kien-truc.md) mục 1, 4; EDA VietJobs (`joblens/docs/project-plan/13`) |
| 2. Kiến trúc và thiết kế | Lambda/Kappa; thành phần, vai trò; sơ đồ luồng, tương tác | 🧪 tư liệu đủ | [`kien-truc.md`](kien-truc.md) (2 sơ đồ), [`adr/0001-lambda-vs-kappa.md`](adr/0001-lambda-vs-kappa.md) |
| 3. Chi tiết triển khai | mã có tài liệu; cấu hình theo môi trường; triển khai, giám sát | ✅ tư liệu đủ | `README.md`, `deploy/README.md` (profile full/lite, Codespaces), `deploy/compat.md`, Grafana, `benchmarks/` |
| 4. Bài học kinh nghiệm | 4 phần × 11 nhóm | 🧪 bản nháp | [`report/bai-hoc-kinh-nghiem.md`](report/bai-hoc-kinh-nghiem.md) – còn 5 chỗ `<cần đo>` |

Gợi ý cho phần 1 ("vì sao hợp dữ liệu lớn"): **khối lượng** (nhiều trang tuyển dụng, tin được crawl lại nhiều lần →
bronze lớn hơn silver), **tốc độ** (tin mới liên tục, cần độ tươi ≤ 2 phút ở bronze), **đa dạng** (lương, địa điểm,
kinh nghiệm ghi tự do, mỗi nguồn một kiểu). Giới hạn: 48.092 tin VietJobs trên cụm một máy; crawler chưa ghép (mục 4
của [`phoi-hop-web-mining.md`](phoi-hop-web-mining.md)).

## 5. Bài học kinh nghiệm – 11 nhóm của giảng viên

| # | Nhóm (theo trang môn học) | Có trong bản nháp | Bổ sung nên viết (sự cố thật 24/10–03/11) |
|---|---|---|---|
| 1 | Thu thập dữ liệu: nhiều nguồn, chất lượng, dữ liệu trễ, trùng lặp và phiên bản | 1.1–1.4 | Thí nghiệm nhiễu (mục 6) |
| 2 | Xử lý Spark: tối ưu job, bộ nhớ, tinh chỉnh partition, tối ưu theo chi phí (CBO) | 2.1–2.3 | Đo AQE / thứ tự join |
| 3 | Xử lý luồng: exactly-once, cửa sổ, state, phục hồi | 3.1–3.3 | Log tiến độ bỏ sót batch (đã sửa in mọi batch) |
| 4 | Lưu trữ: định dạng, phân vùng, nén, dữ liệu nóng/lạnh | 4.1–4.2 | Số file trước/sau compaction (`<cần đo>`) |
| 5 | Tích hợp hệ thống: khám phá dịch vụ, xử lý lỗi, circuit breaker, cân bằng tải | 5.1–5.3 | SQLPad nghe sai cổng do biến môi trường của Service Kubernetes; Trino lưu lược đồ MongoDB đoán từ collection rỗng |
| 6 | Tối ưu hiệu năng: cache, tối ưu truy vấn, cấp phát tài nguyên, nút thắt | 6.0–6.2 | |
| 7 | Giám sát và gỡ lỗi: metrics, cảnh báo, gom log, tìm nguyên nhân gốc | 7.1–7.2 | |
| 8 | Mở rộng: ngang/dọc, tự động, dự trù tài nguyên, chi phí | 8.1 | |
| 9 | Chất lượng dữ liệu và kiểm thử: kiểm tra hợp lệ, unit, tích hợp, hiệu năng | 9.1–9.2 | Thí nghiệm nhiễu: đo precision/recall (mục 6) |
| 10 | Bảo mật và quản trị: phân quyền, mã hóa, nhật ký kiểm toán, tuân thủ | 10.1–10.2 | Công cụ sửa dữ liệu dùng user ứng dụng, mật khẩu trong Secret |
| 11 | Chịu lỗi: phục hồi, nhân bản, sao lưu, khôi phục thảm họa | 11.1–11.3 | `demo-mode.sh` bỏ qua pause khi scheduler chưa sẵn sàng; Codespaces vỡ do khóa GPG kho Yarn |

## 6. Yêu cầu thêm: biến đổi dữ liệu + nhiễu (đã làm – [`thi-nghiem-nhieu.md`](thi-nghiem-nhieu.md), số liệu [`../benchmarks/noise_robustness.md`](../benchmarks/noise_robustness.md))

1. **Tiêm nhiễu có kiểm soát** trong công cụ replay (`vietjobs.py --noise <tỉ lệ> --seed <n>`): định dạng lương
   (`15-25tr`, dấu chấm phân cách, USD), đơn vị/thang đo (lương năm, mất "triệu", đảo min/max), chính tả địa điểm
   (không dấu, viết tắt, tên tỉnh cũ trước sáp nhập 2025), lỗi mã hóa (mojibake, NFD/NFC), giá trị thiếu/giữ chỗ,
   ngoại lai, bản trùng/đến sai thứ tự, message hỏng. Ghi **tệp đáp án** (tin nào bị nhiễu gì, giá trị sạch là gì).
2. **Xử lý nhiễu ở silver:** làm sạch văn bản, phân tích lương bền hơn, so khớp địa điểm bỏ dấu + khoảng cách chỉnh sửa
   trên danh sách alias, phát hiện ngoại lai (khoảng cứng + median ± k·MAD theo cấp bậc × vùng), **bảng cách ly kèm mã
   lý do** thay cho loại bỏ im lặng, thêm kỳ vọng GX về tỉ lệ cách ly/ngoại lai.
3. **Đo:** precision/recall theo loại nhiễu, tỉ lệ khôi phục đúng giá trị sạch, sai số trung vị lương ở gold khi nhiễu
   0/10/20/40% có và không làm sạch → `benchmarks/noise_robustness.md` + ô Grafana.
4. **Ràng buộc:** chạy trong namespace riêng `lab.*` để không làm bẩn bảng thật; cột/bảng mới (cờ ngoại lai, bảng cách
   ly) phải thêm contract ở `joblens` trước và báo nhóm; khử trùng gần giống giữa các trang **không** làm ở đây (của
   Web Mining, ADR 0001 D5) – chỉ xử lý bản trùng chính xác và bản đến sai thứ tự.

## 7. Việc còn lại (theo thứ tự ưu tiên)

| # | Việc | Phục vụ | Trạng thái |
|---|---|---|---|
| 1 | Thí nghiệm nhiễu (mục 6, [`thi-nghiem-nhieu.md`](thi-nghiem-nhieu.md)): tiêm nhiễu → xử lý → đo | Yêu cầu thêm; Spark nhóm 2; bài học nhóm 1, 9 | ✅ cả 3 phần đã chạy trên cụm (robust xử lý đúng ~98% tin nhiễu, baseline ~42%) |
| 2 | Thí nghiệm batch vs streaming lần 2 (không `REUSE_A`, thêm trigger 1 phút) | ADR Kappa | ⬜ |
| 3 | Đo số file bronze trước/sau `iceberg_maintenance` | ADR Kappa (điều kiện 3), bài học 4.1 | ⬜ |
| 4 | Chạy `stream_province_windows --output-mode append`, ghi `deploy/compat.md` | Spark nhóm 5 | ⬜ |
| 5 | Đo tối ưu nhiều join: AQE bật/tắt, thứ tự join, so kế hoạch + thời gian | Spark nhóm 3 | ⬜ |
| 6 | Hỏi giảng viên: có bắt buộc GraphFrames không; nếu có, làm đồ thị trên dữ liệu của BD (mục 6 của [`phoi-hop-web-mining.md`](phoi-hop-web-mining.md)) | Spark nhóm 6 | ⬜ |
| 7 | Chuyển ADR 0001 (Lambda/Kappa) sang "Đã chấp nhận" sau việc 2, 3 | Báo cáo phần 2 | 🧪 |
| 8 | Điền 5 chỗ `<cần đo>` + viết các bài học bổ sung ở mục 5 | Báo cáo phần 4 | 🧪 |
| 9 | Viết báo cáo phần 1–3 thành văn (không chép nguyên văn giữa các báo cáo môn – R3; không dùng chung hình – R5) | Báo cáo | ⬜ |
| 10 | Ghép với Web Mining theo [`phoi-hop-web-mining.md`](phoi-hop-web-mining.md) (crawler → Kafka, image mô hình, mart gold còn thiếu) | Hệ thống hoàn chỉnh | ⬜ |
| 11 | Slide + video dự phòng cho buổi thuyết trình (tuần 13–15) | Thuyết trình | ⬜ |
| 12 | Hỏi giảng viên: lịch thuyết trình của nhóm, hạn nộp báo cáo, thang điểm | – | ⬜ |
| 13 | README: điền thành viên, MSSV, vai trò, ngày giảng viên đồng ý | Bài nộp | ⬜ |
