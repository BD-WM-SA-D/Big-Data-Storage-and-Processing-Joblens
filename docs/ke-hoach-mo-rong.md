# Kế hoạch mở rộng sau góp ý của giảng viên (2026-11)

Góp ý của giảng viên sau buổi kiểm tra: (1) **sáng tạo hơn**, (2) hệ thống gần **mức production** hơn, (3) quan tâm
nhất đến **biến đổi dữ liệu** (*data transformation*). Tài liệu này ghi các hướng đã cân nhắc, nối mỗi hướng với
bài giảng của môn và với khoảng trống thực tế trong repo, rồi chốt thứ tự làm. Trạng thái từng việc theo dõi ở
mục 7 của [`yeu-cau-mon-hoc.md`](yeu-cau-mon-hoc.md).

Nguồn đối chiếu: slide bài giảng 2026 trên Google Drive của môn (L01–L10; L11–L14 chưa đăng lúc đọc, 2026-11-10).
Tài liệu chỉ ghi số trang và ý chính, **không** chép nội dung slide vào repo.

## 1. Hiện trạng (khoảng trống tìm thấy khi rà repo 2026-11-10)

| Khía cạnh | Hiện có | Khoảng trống |
|---|---|---|
| Biến đổi | bronze → `silver.job_postings` (chuẩn hóa lương, địa điểm, kinh nghiệm, cấp bậc; xử lý nhiễu) | Gold mỏng: dbt chỉ có 2 model (`dim_location`, `mart_salary_role_region`), đều `materialized: table` (dựng lại toàn bộ mỗi lần); chưa có bảng lịch sử thay đổi tin; USD quy đổi theo một tỷ giá cố định (`DEFAULT_USD_VND`) |
| Kafka | Strimzi KRaft, topic khai báo bằng `KafkaTopic` | 1 broker, `replicas: 1`, `min.insync.replicas: 1` – mất broker là dừng hệ thống |
| Cảnh báo việc làm | Đã có topic `app.alert_subscriptions.v1` và `alerts.job_match.v1` (ADR 0001 D4) | Chưa có job streaming ghép cảnh báo |
| Giám sát | Grafana (dashboard demo), Kafka UI | Chưa có Prometheus/luật cảnh báo; chưa có mục tiêu độ trễ (SLO) |
| CI | pre-commit + pytest chạy tay | Chưa có `.github/workflows` |
| Spark | Executor cố định | Chưa bật dynamic allocation |

## 2. Hướng biến đổi dữ liệu (ưu tiên cao nhất)

| # | Việc | Nối với bài giảng | Bằng chứng sẽ có |
|---|---|---|---|
| T1 | **Lịch sử tin tuyển dụng (SCD loại 2)**: từ `silver.job_postings_snapshot` dựng `silver.job_postings_history` – mỗi phiên bản của một tin một dòng, `valid_from` / `valid_to` / `is_current`. Làm bằng Iceberg `MERGE INTO` (Spark) hoặc `dbt snapshot`; phát hiện thay đổi lương, cấp bậc, địa điểm, tin đóng/đăng lại | L04 (upsert/CDC, time travel, ACID) | Câu hỏi mới trả lời được: tin mở bao lâu, tin nào tăng lương sau N ngày, tỉ lệ đăng lại; demo một dòng đổi phiên bản trước mặt giảng viên |
| T2 | **Gold tăng dần**: chuyển model dbt sang `incremental` (hoặc đọc changelog của Iceberg), thêm mart mới (nhu cầu tuyển theo tuần × tỉnh × cấp bậc, thời gian tin mở theo ngành) | L04, L09 (tái xử lý) | Bảng đo dựng lại toàn bộ vs tăng dần: thời gian, số byte quét – `benchmarks/` |
| T3 | **Replay lịch sử bằng logic mới (Kappa)**: đặt lại offset Kafka, chạy bộ chuẩn hóa robust vào một **nhánh Iceberg**, so với bảng hiện tại, GX làm cổng, đạt thì publish (write-audit-publish) | L09 tr. 28 (tái xử lý bằng replay log) | Số dòng khác nhau giữa hai phiên bản, kết quả GX, thời gian replay. Cần kiểm tra retention của topic đủ giữ toàn bộ lịch sử |
| T4 | **Biến đổi trên luồng**: gom `events.user.v1` thành phiên (`session_window`), tính phễu xem → lưu → ứng tuyển theo ngày × nhóm việc | L08 tr. 46–52 (lọc/join/gộp trên luồng), L11 | Bảng phễu + so watermark/độ trễ |
| T5 | **Quy đổi USD theo tỷ giá đúng ngày đăng tin** (join "as-of" với bảng tỷ giá) thay cho tỷ giá cố định | – | Kiểm thử cho trường hợp ngày không có tỷ giá; bảng tỷ giá là fixture ghi rõ nguồn và ngày lấy, không crawl |
| T6 | **Bố cục vật lý**: sort order / Z-order theo tỉnh × ngày, partition evolution trên silver | L03 tr. 29 (min/max của Parquet cho phép bỏ qua row group) | Số file / byte Trino quét cho cùng truy vấn trước và sau |
| T7 | **Mart dùng dữ liệu Web Mining** (khi `silver.job_skills` có): nhu cầu kỹ năng theo tháng × tỉnh. Repo này chỉ **đọc** bảng đó | – | Mart gold + câu truy vấn demo |

**Chốt:** làm **T1 → T2 → T6** trước (cùng một câu chuyện: thô → sạch → lịch sử → mart tăng dần → bố cục tối ưu; đều
là batch, nhẹ RAM, nằm trọn trong repo này). Còn thời gian thì thêm **T4**. T3 làm sau khi kiểm tra retention.

## 3. Công nghệ của các công ty lớn được nhắc trong slide

| # | Việc | Slide | Ghi chú |
|---|---|---|---|
| P1 | **Kafka theo cấu hình bền vững** của slide: 3 broker, replication 3, `min.insync.replicas=2`, `acks=all`, producer idempotent; demo tắt một broker giữa luồng, đếm dòng trước/sau | L08 tr. 27, 35–36 | Thêm khoảng 1–2 GB RAM. Đổi cấu hình dùng chung → báo nhóm |
| P2 | **Cảnh báo việc làm thời gian thực** (ghép đăng ký × tin mới → `alerts.job_match.v1`) – bài toán cùng dạng "Uber matching" mà slide nêu; làm bằng Spark hoặc so với ksqlDB | L08 tr. 8, 46–52; ADR 0001 D4 | Giao diện cảnh báo thuộc PTTKHT, repo này chỉ ghi topic |
| P3 | **Kho phục vụ kiểu Dynamo**: bản nhẹ là MongoDB replica set 3 thành viên, demo write concern / read concern và CAP; bản nặng là Cassandra 3 nút với N=3, R=2, W=2, tắt một nút vẫn ghi được | L06 (cả bài), L05 tr. 28, 45; L09 tr. 11 | Cassandra ~1 GB RAM/nút – máy hiện tại đã sát giới hạn, nên chọn bản MongoDB |
| P4 | **SLO độ trễ theo phân vị 99,9** (tin vào Kafka → có trong Trino / phát cảnh báo) | L06 tr. 9 | Nhỏ; đi cùng P1/P2 |
| P5 | **Trino resource groups** tách truy vấn tương tác và ETL; phân tích A/B trên `events.user.v1` | L07 tr. 9–11 | Biến thể xếp hạng thuộc Web Mining; repo này chỉ phân tích sự kiện |
| P6 | Redis cache cho truy vấn dashboard nóng | L05 tr. 24 | Ưu tiên thấp |

## 4. Mức production

Đi kèm P1–P4: Prometheus + luật cảnh báo (dữ liệu cũ hơn N phút, lag vượt ngưỡng, GX lỗi) gửi Telegram/email;
GitHub Actions chạy pre-commit, pytest, `dbt build` trên fixtures, kiểm tra manifest; Spark dynamic allocation;
`KafkaUser` + NetworkPolicy; quy tắc expire snapshot + sao lưu bucket + một lần thử khôi phục; runbook "X hỏng thì làm
gì". Trên laptop không đạt quy mô production – mục tiêu là **chứng minh thực hành production bằng số đo**.

## 5. Ràng buộc

- Bảng/topic mới (`silver.job_postings_history`, mart gold mới, keyspace hoặc collection phục vụ) thêm contract ở
  `joblens` **trước** – chỉ thêm, không đổi tên/xóa – rồi nhắn nhóm chat.
- Không ghi `silver.job_skills`, `silver.job_dedup`, `silver.review_aspects`, `silver.reco_scores`.
- Không bịa số đo: mọi kết quả ghi `<cần đo>` cho tới khi chạy trên cụm, rồi thêm dòng vào `deploy/compat.md`.
- Theo dõi RAM: hiện cụm đã từng OOM với Spark chế độ local – mỗi việc thêm dịch vụ phải ghi mức RAM đo được.
