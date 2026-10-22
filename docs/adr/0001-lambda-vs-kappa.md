# ADR 0001 – Lambda hay Kappa cho đường nạp dữ liệu tin tuyển dụng

- **Trạng thái:** Đề xuất – chờ số đo (`benchmarks/batch_vs_streaming.md`)
- **Ngày:** 2026-10-22
- **Người viết:** `<tên>` · **Người kiểm tra:** `<người kiểm tra>`
- **Liên quan:** `joblens/docs/project-plan/03` (mục "Batch vs streaming", "Metrics nền tảng"),
  `joblens/docs/adr/0001-ranh-gioi-xu-ly-giua-cac-repo.md`

## Bối cảnh

Tin tuyển dụng đến từ crawler (theo đợt, vài nghìn tin/ngày mỗi nguồn) và từ bộ dữ liệu lịch sử VietJobs
(48.092 tin, nạp một lần). Người dùng cổng JobLens cần tin mới xuất hiện trong tìm kiếm và cảnh báo việc làm
(`alerts.job_match.v1`) nhanh; báo cáo lương theo tỉnh/cấp bậc (gold) chỉ cần cập nhật theo giờ.

Hai kiến trúc kinh điển:

- **Lambda:** hai đường song song – batch (đầy đủ, tính lại toàn bộ) và speed layer (streaming, xấp xỉ, gần
  thời gian thực); tầng phục vụ ghép kết quả hai đường. Mỗi phép biến đổi phải viết và giữ đúng ở hai nơi.
- **Kappa:** một đường duy nhất là luồng sự kiện có thể phát lại (Kafka); muốn tính lại thì phát lại luồng
  (replay) qua cùng một mã xử lý.

## Hiện trạng đã dựng

- Bronze `bronze.jobs_raw` (Iceberg, append-only) nhận dữ liệu từ **cả hai đường**: streaming `bronze_jobs`
  (Kafka → Spark Structured Streaming) và batch `vietjobs.py --sink bronze` (nạp lịch sử). Cùng một hàm
  `to_bronze` dùng cho cả hai nên không phải viết hai lần logic.
- Silver/gold chạy **batch theo lịch** (DAG `joblens_pipeline`, mỗi giờ): `MERGE INTO` theo `job_uid` nên kết quả
  không phụ thuộc dòng đến từ đường nào hay đến mấy lần (idempotent).
- Tái xử lý: Kafka giữ 7 ngày (`jobs.raw.*.v1`) để replay; Iceberg giữ snapshot để time travel; job silver có
  `--full` để tính lại từ toàn bộ bronze.

## Phương án

| | A. Lambda | B. Kappa thuần | C. Kappa cho nạp + batch theo lịch cho biến đổi (hiện trạng) |
|---|---|---|---|
| Đường nạp bronze | batch + streaming song song, ghép ở serving | chỉ streaming; lịch sử nạp bằng replay vào Kafka | streaming là đường chính; batch chỉ để nạp lịch sử/khôi phục, ghi cùng một bảng |
| Silver/gold | hai bản (batch + speed) | streaming liên tục | batch mỗi giờ, MERGE idempotent |
| Độ tươi bronze (p95) | batch: ≤ chu kỳ + 122 s (lần 1) | 60,6 s (lần 1) | 60,6 s (lần 1) |
| Độ tươi gold | speed layer: giây–phút | giây–phút | ≤ chu kỳ DAG + thời gian chạy (`<số đo>`) |
| Chi phí thường trực | cao nhất (hai hệ) | job streaming chạy liên tục cho mọi tầng | một job streaming nhỏ (~1.377 MiB khi không tải, lần 1) + job batch theo giờ |
| Số file nhỏ | nhiều | nhiều nhất | 2 file / 5.000 tin ở bronze (lần 1, trigger 10 s), gộp bằng `iceberg_maintenance` |
| Độ phức tạp vận hành | cao: hai mã, ghép kết quả | trung bình: mọi biến đổi là streaming, khó với MERGE/GX/dbt | thấp–trung bình: một mã nạp, dbt/GX chạy batch quen thuộc |
| Tái xử lý | chạy lại batch | replay Kafka (giới hạn bởi retention) | replay Kafka hoặc `--full` trên bronze + time travel |

## Quyết định (đề xuất)

Chọn **C – Kappa cho đường nạp, batch theo lịch cho silver/gold**, với điều kiện số đo trong
`benchmarks/batch_vs_streaming.md` cho thấy:

1. đường B đạt chỉ tiêu độ tươi bronze ≤ 2 phút (docs/project-plan/03) ở tải chuẩn – lần 1: p95 = 60,6 s ở 100 tin/giây ✔;
2. hai đường cho cùng tập khóa (`độ đúng` = 0) – lần 1: 0 ✔;
3. số file nhỏ của B được `iceberg_maintenance` gộp lại trong một chu kỳ DAG – `<trước → sau>` (chưa đo).

Lý do (định tính, chờ số đo xác nhận): một mã nạp duy nhất cho cả dữ liệu sống và dữ liệu lịch sử (tinh thần
Kappa); silver/gold cần `MERGE INTO`, kiểm thử dbt và cổng GX – dễ làm đúng và kiểm tra ở chế độ batch; báo cáo
lương không cần độ tươi dưới một giờ. Nhu cầu gần thời gian thực (cảnh báo việc làm) sẽ đọc trực tiếp từ luồng
Kafka bằng job streaming riêng (ADR 0001 D4 của repo `joblens`), không cần speed layer cho gold.

## Hệ quả

- Phải giữ job streaming `bronze_jobs` chạy liên tục (chi phí thường trực) và theo dõi lag Kafka.
- Đường batch vẫn tồn tại nhưng chỉ cho nạp lịch sử – phải ghi đúng `kafka_topic = 'backfill.<nguồn>'` để phân biệt.
- Retention Kafka (7 ngày) giới hạn khả năng replay; tái xử lý xa hơn dùng `--full` trên bronze.
- Nếu sau này cần gold gần thời gian thực, xem lại phương án B cho riêng mart đó.

## Cần làm trước khi chuyển trạng thái "Đã chấp nhận"

- [x] Lần 1: trigger 10 giây, 100 tin/giây (`benchmarks/batch_vs_streaming.md`).
- [ ] Lần đo đầy đủ có chi phí của đường A; lần với trigger 1 phút; đo số file trước/sau `iceberg_maintenance`.
- [ ] Điền các ô `<…>` ở trên bằng số đo, dẫn link tới lần đo.
- [ ] Thêm ADR vào danh mục `joblens/adr-index.md` (repo chung).
