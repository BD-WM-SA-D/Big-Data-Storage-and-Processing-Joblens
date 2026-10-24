# Đối chiếu yêu cầu bài tập lớn IT4931 với nền tảng JobLens

Nguồn yêu cầu: trang môn học của giảng viên – <https://datalab.vn/teaching/it4931-luu-tru-va-xu-ly-du-lieu-lon>
(mục "Hướng dẫn bài tập lớn"; bản tiếng Anh IT4043E có cùng nội dung). Trang không ghi thang điểm, hạn nộp hay yêu
cầu demo cụ thể – **hỏi lại giảng viên** những phần này. Bảng dưới dùng khi viết báo cáo và khi trình bày: mỗi dòng chỉ
ra *file* và *bước demo* chứng minh yêu cầu.

Trạng thái (cập nhật 2026-10-23 sau lần chạy DAG đầy đủ – số liệu ở `benchmarks/spark_analytics.md`): ✅ có và đã chạy trên cụm · 🧪 có mã + kiểm thử, chưa chạy trên cụm · ⬜ chưa có

## 1. Yêu cầu chung và công nghệ

| Yêu cầu | Trạng thái | Ở đâu | Demo (docs/demo.md) |
|---|---|---|---|
| Hệ thống hoàn chỉnh giải bài toán thực tế | ✅ | Tin tuyển dụng VN (VietJobs 48.092 tin) → lương/cấp bậc/tỉnh | mục 3, phút 0–2 |
| Kiến trúc Lambda **hoặc** Kappa | ✅ (ADR nháp) | `docs/adr/0001-lambda-vs-kappa.md`, số đo `benchmarks/batch_vs_streaming.md` | phút 13–15 |
| Pipeline thu thập → xử lý → lưu trữ → trực quan hóa | ✅ | Kafka → Spark → Iceberg/MongoDB → Trino → Grafana | toàn bộ |
| Xử lý: Apache Spark | ✅ | `spark-jobs/` (Spark 4.1.3, PySpark) | |
| Lưu trữ phân tán: HDFS hoặc tương đương | ✅ | SeaweedFS (S3 API, thay HDFS) + Apache Iceberg – `deploy/storage/` | trình duyệt SeaweedFS |
| Hàng đợi: Kafka/RabbitMQ… | ✅ | Strimzi Kafka 4.3.1 KRaft – `deploy/kafka/` | Kafka UI |
| Cơ sở dữ liệu NoSQL | ✅ | MongoDB 8.0 – `deploy/mongodb/`, collection `province_windows`, `analytics_*` | ô Grafana MongoDB |
| Triển khai: Kubernetes hoặc Cloud (không chỉ Docker) | ✅ / 🧪 | k3d trên máy; GitHub Codespaces – `docs/cloud-codespaces.md` | `k9s` / `kubectl get pods -A` |

## 2. Kỹ năng Spark (mức trung cấp)

| Nhóm | Kỹ thuật | Trạng thái | Ở đâu |
|---|---|---|---|
| Tổng hợp phức tạp | Window functions | ✅ | `silver_job_postings.py` (bản crawl mới nhất mỗi job_uid); `analytics_spark.py` (dense_rank trong vùng, tỉ trọng cộng dồn, trung bình trượt 3 giờ) |
| | Pivot / unpivot | ✅ | `analytics_spark.level_region_pivot` (vùng × cấp bậc, unpivot đối chiếu tổng) |
| | Hàm tổng hợp tùy biến | ✅ | `analytics_spark.trimmed_mean` (pandas UDF tổng hợp) |
| Biến đổi nâng cao | Nhiều giai đoạn, chuỗi thao tác | ✅ | Kafka → parse → chuẩn hóa → MERGE (`silver_job_postings.py`) |
| | UDF cho logic nghiệp vụ | ✅ | `normalize_udf` (lương, 34 tỉnh, kinh nghiệm, cấp bậc) |
| Join | Broadcast join (dữ liệu lệch) | ✅ | `analytics_spark.explode_locations` – tin ⋈ 34 tỉnh; Hà Nội/TP.HCM chiếm phần lớn tin |
| | Sort-merge join | ✅ | `analytics_spark.activity_join` – silver ⋈ snapshot theo job_uid (hint MERGE) |
| | Tối ưu nhiều join | 🧪 | AQE bật trong `analytics_spark.py`; kế hoạch lưu S3 để so sánh |
| Hiệu năng | Partition pruning | ✅ | bronze `days(ingest_ts), source`; silver đọc tăng dần theo mốc; `analytics_spark.ingest_hourly` kiểm tra bộ lọc đẩy xuống |
| | Bucketing | ✅ | silver `bucket(16, job_uid)` |
| | Cache / persist | ✅ | `bronze_jobs.py` (persist mỗi micro-batch); `analytics_spark.py` |
| | Phân tích kế hoạch thực thi | ✅ | `analytics_spark.plan_text` → `s3://joblens/reports/spark-plans/` + cờ `plan_checks` trên Grafana |
| Streaming | Structured Streaming | ✅ | `bronze_jobs.py`, `stream_province_windows.py` |
| | Output modes | 🧪 | `stream_province_windows.py --output-mode update\|append` (đã chạy `update` trên cụm; `append` chưa chạy) |
| | Watermark, dữ liệu trễ | ✅ | `stream_province_windows.py` (watermark 10 phút); replay `--late-every` tạo tin trễ; kiểm thử `tests/test_stream_province_windows.py` |
| | Quản lý state | ✅ | aggregation theo cửa sổ (state trong checkpoint, log `numRowsTotal`) |
| | Exactly-once | ✅ | checkpoint Kafka offset + commit Iceberg (bronze); upsert idempotent theo `_id` (MongoDB) |
| Phân tích nâng cao | Spark MLlib | ✅ | `analytics_spark.salary_model` – Pipeline + CrossValidator, so với mô hình đoán trung vị |
| | Thống kê, chuỗi thời gian | ✅ | tương quan kinh nghiệm–lương, phân vị; lượng nạp theo giờ |
| | GraphFrames | ⬜ | không làm (đồ thị kỹ năng là bài của repo Web Mining) |

## 3. Báo cáo

| Phần | Trạng thái | Nguồn |
|---|---|---|
| 1. Định nghĩa bài toán | ⬜ | `joblens/docs/project-plan/01`, EDA VietJobs |
| 2. Kiến trúc và thiết kế (Lambda/Kappa, sơ đồ luồng) | 🧪 | `docs/adr/0001-lambda-vs-kappa.md`, `joblens/docs/project-plan/03` |
| 3. Chi tiết triển khai (mã, cấu hình theo môi trường, triển khai, giám sát) | ✅ | `deploy/README.md` (profile full/lite, Codespaces), `deploy/compat.md`, Grafana |
| 4. Bài học kinh nghiệm – 11 nhóm, mẫu 4 phần | 🧪 (bản nháp) | `docs/report/bai-hoc-kinh-nghiem.md` |
