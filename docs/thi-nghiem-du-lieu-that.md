<!-- LIÊN QUAN: Web-mining-joblens/crawler/producer.py, Web-mining-joblens/batch/tables.py, Web-mining-joblens/models.yaml – đổi tham số producer hay image thì cập nhật file này -->
# Thí nghiệm F: dữ liệu crawl thật của Web Mining đi qua toàn bộ nền tảng

Tới nay mọi số đo của nền tảng chạy trên VietJobs (một lần nạp) và dữ liệu mô phỏng (`recrawl_sim`, nhiễu). Thí nghiệm
này cho dữ liệu **crawl thật** của Web Mining (TopCV, ITviec – nhiều đợt crawl có ngày, kèm review công ty) đi đúng
đường của hệ thống thật: crawler → Kafka → bronze → silver → lịch sử → image mô hình WM → gold, rồi đối soát từng tầng.
Phần đo độ chính xác mô hình (F1, nDCG…) thuộc báo cáo Web Mining, **không** lặp lại ở đây; thí nghiệm này hỏi nền tảng
có di chuyển, lưu, xử lý dữ liệu thật đúng và kịp không.

## 1. Câu hỏi và giả thuyết

| # | Câu hỏi | Đo bằng | Đạt khi |
|---|---|---|---|
| F1 | **Không mất, không thừa:** mọi message crawler gửi có được tính ở đâu đó không? | `gửi = bronze + bronze.ingest_errors` (tin, review); `dlq.parse_errors.v1` = lỗi parse WM gửi | chênh 0 |
| F2 | **Gửi lại không sinh trùng:** gửi lại cùng file (producer idempotent, MERGE theo `job_uid`) | số dòng silver, history, bảng WM trước/sau lần gửi thứ hai | silver/history/WM không đổi; bronze tăng đúng số message (bronze là log, giữ mọi lần nhận) |
| F3 | **Chất lượng chuẩn hóa trên dữ liệu thật** so với VietJobs | % có lương, địa điểm, cấp bậc, kinh nghiệm, ngày đăng (`report-real-ingest.sh` mục 4); GX `gx_silver` | GX 15/15; độ phủ ghi lại, chỗ thấp thì tìm nguyên nhân (ví dụ cách ghi lương riêng của trang) |
| F4 | **Lịch sử thật:** các đợt crawl có ngày cho thấy tin đổi gì | `silver.job_postings_history`: % tin đổi, trường đổi (mục 5) | bất biến SCD2 đạt (dbt test); so tỉ lệ đổi thật với giả định 5%/lần crawl của `recrawl_sim` |
| F5 | **Độ trễ khi nạp dạng luồng:** gửi từng đợt crawl theo thứ tự ngày khi `bronze_jobs` streaming (trigger 10 giây) đang chạy | thời gian từ lúc gửi một đợt tới khi bronze có đủ dòng (`ingest-crawl.sh --stream`) | trễ ≤ 2 trigger + thời gian commit (dự kiến < 60 s – `<cần đo>`) |
| F6 | **Ghép mô hình đúng:** kết quả image WM chạy trong DAG khớp con số WM tự báo cáo | `report-real-ingest.sh` mục 6 – số nhóm tin trùng, số nhóm khác trang | khớp báo cáo tiến độ WM (17 nhóm trùng, 5 nhóm khác trang trên 888 tin) khi cùng dữ liệu, cùng phiên bản image; lệch thì tìm nguyên nhân |
| F7 | **Gold có số liệu thật:** mart nhu cầu kỹ năng, đánh giá công ty | mục 7 | các bảng có dòng; top kỹ năng hợp lý; test dbt đạt |

## 2. Chuẩn bị

| Việc | Ai | Ghi chú |
|---|---|---|
| Thư mục `data/crawl/` của WM (jobs.jsonl theo đợt, reviews.jsonl, parse_errors.jsonl) | WM gửi qua kênh nội bộ | Không commit, không gửi ra dịch vụ ngoài; đã lọc dữ liệu cá nhân ở crawler |
| Cụm chạy, `deploy/kafka` đã áp listener `local` (9094) | BD | `bash deploy/kafka/install.sh` (broker khởi động lại một lần) |
| Image mô hình | BD build `stub`; WM gửi bản thật | `bash deploy/mining/install.sh stub` → thử đường đi; bản thật: `full` (skill-extractor `ml`, job-dedup + absa-scorer `light`) |
| Mã producer | tự lấy | `scripts/ingest-crawl.sh` clone WM đúng `WM_REF` (`deploy/mining/images.env`) và `uv sync --group kafka` |

## 3. Cách chạy

| # | Terminal | Lệnh | Thành công khi |
|---|---|---|---|
| 1 | T1 | `bash scripts/demo-mode.sh streaming` | DAG tạm dừng, có RAM cho Spark |
| 2 | T1 | `bash scripts/ingest-crawl.sh <data/crawl> --dry-run` | in số message theo topic, không lỗi schema (lỗi → DLQ) |
| 3 | T2 | `bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"` | log micro-batch đều đặn |
| 4 | T1 | `bash scripts/ingest-crawl.sh <data/crawl> --stream 30` | bảng "Đợt crawl – Tin gửi – Trễ" (F5) |
| 5 | T1 | `EXECUTORS=0 bash deploy/spark/submit.sh bronze_reviews` | log `đọc N message review hợp lệ` |
| 6 | T1 | `EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh silver_job_postings` rồi `… silver_job_history` | history: `… tin từng thay đổi`, không `Sai bất biến` |
| 7 | T1 | `EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh gx_silver` | 15/15 (F3) |
| 8 | T1 | `bash deploy/mining/install.sh stub` (hoặc `full`) → Airflow Trigger `joblens_pipeline` | 3 task `mining_*` xanh, `dbt_build_gold` xanh |
| 9 | T1 | `bash scripts/report-real-ingest.sh` | bảng mục 1–7 → điền kết quả F1, F3, F4, F6, F7 |
| 10 | T1 | gửi lại lần hai: `bash scripts/ingest-crawl.sh <data/crawl>`, chạy lại bước 5–6, `report-real-ingest.sh` | F2: silver/history không đổi |

Chế độ lô (bỏ `--stream`) gửi mọi file một lần – dùng khi chỉ cần dữ liệu, không đo trễ; sau đó
`bronze_jobs --once` nếu streaming không chạy.

## 4. Vì sao thiết kế như vậy

- **Đi qua Kafka chứ không nạp thẳng bronze** (khác backfill VietJobs): đây là đường của hệ thống thật (ADR 0001 D1:
  crawler → Kafka). Đo được cả hợp đồng message (schema, DLQ) lẫn exactly-once của bronze streaming.
- **Gửi theo thứ tự đợt crawl** thay vì một lần: mô phỏng crawler chạy hằng ngày để đo độ trễ nạp (F5). Lịch sử (F4)
  không phụ thuộc thứ tự gửi vì `silver_job_history` sắp theo `crawl_ts` – thí nghiệm kiểm luôn tính chất này trên dữ
  liệu thật.
- **Port-forward listener riêng** (`local`, quảng bá `localhost:9094`) thay vì mở NodePort: không phải tạo lại cụm k3d,
  không mở cổng ra mạng. Giới hạn: chỉ một broker, chỉ máy đang port-forward gửi được.
- **Đối soát bằng số đếm ở mọi tầng** (F1): cách kiểm "không mất dữ liệu" rẻ và thuyết phục nhất cho báo cáo.
- **Chỉ đưa nguồn crawl vào mô hình** (`--sources topcv,itviec`): dữ liệu mô phỏng `synthetic` trùng tiêu đề + công ty
  với tin VietJobs gốc, đưa vào thì job-dedup gộp sai (WM 0.1.1 có tham số này theo đề nghị của BD).

## 5. Kết quả

`<cần đo>` – điền sau khi chạy (ngày, commit BD, `WM_REF`, bản image stub/full), thêm một dòng `deploy/compat.md`.

| # | Kết quả | Đạt? |
|---|---|---|
| F1 | `<cần đo>` | |
| F2 | `<cần đo>` | |
| F3 | `<cần đo>` | |
| F4 | `<cần đo>` | |
| F5 | `<cần đo>` | |
| F6 | `<cần đo>` | |
| F7 | `<cần đo>` | |

## 6. Rủi ro đã biết

- `absa-scorer --changed-since` lọc theo `crawl_ts` (thời điểm crawl): review crawl từ trước rồi mới gửi vào Kafka sẽ
  bị bỏ sót nếu chạy tăng dần → DAG **không** truyền tham số này cho absa (xử lý lại toàn bộ, review ít). Đề nghị WM đổi
  mốc sang `ingest_ts` của bronze.
- Retention các topic `jobs.raw.*` là 7 ngày: Kafka không phải nơi lưu lâu dài; tái xử lý lịch sử dựa trên bronze.
- Bản `ml` của skill-extractor (PhoBERT CPU, image ~3,4 GB) giới hạn 4 Gi RAM trong DAG; máy 8 GB không nên chạy cùng
  Spark – đo RAM thật ở bước 8.
