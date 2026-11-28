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
| F6 | **Ghép mô hình đúng:** image WM chạy trong DAG cho cùng kết quả như WM chạy image đó ngoài cụm | `report-real-ingest.sh` mục 6 – số dòng kỹ năng, số nhóm tin trùng, số câu review | khớp kết quả WM chạy cùng image `17b4730` trên cùng file (WM gửi số đếm). Báo cáo WM đã bỏ phần dedup (23/11) nên không so với con số 17 nhóm cũ |
| F7 | **Gold có số liệu thật:** mart nhu cầu kỹ năng, đánh giá công ty | mục 7 | các bảng có dòng; top kỹ năng hợp lý; test dbt đạt |

## 2. Dữ liệu nhận ngày 28/11 và kiểm tra trước khi nạp

WM gửi `joblens-wm-crawl-2026-11-28.zip` (commit WM `17b4730`), chỉ dùng nội bộ nhóm, xóa khi kết thúc môn; dữ liệu
ITviec chưa có văn bản đồng ý của ITviec. Hai file image bản thật (`light` ~250 MB, `ml` ~930 MB) gửi riêng.

| File | Message | Khóa khác nhau | Lỗi schema |
|---|---|---|---|
| `topcv/2026-10-12/jobs.jsonl` | 304 | 304 | 0 |
| `topcv/2026-11-15/jobs.jsonl` | 27 | 27 | 0 |
| `topcv/2026-11-18/jobs.jsonl` | 21 | 21 | 0 |
| `itviec-jobs/2026-11-13/jobs.jsonl` | 561 | **536** | 0 |
| `itviec/reparse-2026-11-15/reviews.jsonl` | 144 review | 144 | 0 |

Kiểm tra trên máy (JSON Schema `job_posting.v1`/`review.v1`; chạy chuẩn hóa và `silver_job_history` bằng Spark cục bộ
trên đúng các file này, không nạp cụm) tìm ra trước ba điều – thí nghiệm trên cụm sẽ xác nhận bằng số đếm:

1. **Id tin ITviec không duy nhất.** 24 `source_job_id` (ví dụ `2706`) trỏ tới hai tin khác hẳn nhau (khác URL, tiêu
   đề, công ty): crawler lấy số cuối của slug URL làm id. Trên nền tảng: silver ghi đè tin này bằng tin kia (mất 25 tin
   ITviec), lịch sử sinh **24 "thay đổi" giả** (đổi tiêu đề + công ty). `report-real-ingest.sh` mục 2b phát hiện được.
   Sửa ở WM: dùng cả slug làm id. Bộ "seen" của spider cũng theo id này nên tin mới có số trùng sẽ bị bỏ qua.
2. **Ba đợt crawl TopCV không có tin chung** (0 tin xuất hiện ở hai đợt): spider chỉ lấy tin chưa thấy, nên lịch sử
   tin thật (F4) dự kiến **0 thay đổi**. Muốn đo lịch sử thật thì crawler phải ghé lại tin đã có (crawl lại theo
   `lastmod`), không chỉ tin mới.
3. **Địa điểm ITviec chỉ có quận/huyện** ("Quận Tân Bình", "Huyện Gia Lâm") không kèm tỉnh: chuẩn hóa cũ chỉ nhận
   32,1% tin ITviec. Đã sửa ở repo này (bỏ tiền tố "Quận/Huyện" rồi tra alias quận/huyện; thêm quận/huyện Đà Nẵng vào
   `dbt/seeds/locations_2025.csv`) → 73,8%; phần còn lại là `Not Available` (152 tin) do nguồn không ghi. TopCV 99,7%.
   Đổi `normalize.py` nên chạy `silver_job_postings --full` sau khi cập nhật (bài học 21/11).

## 3. Chuẩn bị

| Việc | Ai | Ghi chú |
|---|---|---|
| Thư mục `data/crawl/` của WM (jobs.jsonl theo đợt, reviews.jsonl, parse_errors.jsonl) | WM gửi qua kênh nội bộ | Không commit, không gửi ra dịch vụ ngoài; đã lọc dữ liệu cá nhân ở crawler |
| Cụm chạy, `deploy/kafka` đã áp listener `local` (9094) | BD | `bash deploy/kafka/install.sh` (broker khởi động lại một lần) |
| Image mô hình | BD build `stub`; WM gửi bản thật | `bash deploy/mining/install.sh stub` → thử đường đi; bản thật: `full` (skill-extractor `ml`, job-dedup + absa-scorer `light`) |
| Mã producer | tự lấy | `scripts/ingest-crawl.sh` clone WM đúng `WM_REF` (`deploy/mining/images.env`) và `uv sync --group kafka` |

## 4. Cách chạy

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

## 5. Vì sao thiết kế như vậy

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

## 6. Kết quả

### Lần 1 – 2026-11-28, image stub

BD `1fe3a9e`, WM `17b4730`, image mô hình **stub**, dữ liệu nhận 28/11 (mục 2). Đối soát lúc 22:51 (sau lần gửi
đầu) và 23:05 (sau lần gửi lại), giờ Việt Nam. Dòng tương ứng trong `deploy/compat.md`.

| # | Kết quả | Đạt? |
|---|---|---|
| F1 | Kafka ↔ bronze khớp từng message: topcv 352, itviec 561, review 144. Có 0 dòng cách ly, 0 message `dlq.parse_errors.v1`. Topic `jobs.raw.topcv.v1` có thêm 6 message `synthetic` từ smoke test 17/10, vẫn có trong bronze. Tính cả 6 message này thì 358 = 352 + 6 | Đạt |
| F2 | Gửi lại toàn bộ file: bronze tăng đúng số message (itviec 561 → 1.122, topcv 352 → 704, review 144 → 288), Kafka cũng vậy (710 = 704 + 6). Silver, lịch sử, bảng WM và gold có số đếm **giống hệt** lần đầu. Silver chạy lại lúc 22:58–22:59, giữa hai lần đối soát. F2 cho bảng WM cần thêm một lượt DAG sau lần gửi lại – chưa xác nhận | Đạt (silver, lịch sử) |
| F3 | GX `<cần xác nhận>`. Độ phủ – topcv: lương 54,3 % + thỏa thuận 45,5 %, địa điểm 99,7 %, cấp bậc 96,6 %, kinh nghiệm 91,2 %, ngày đăng 100 %. itviec: lương 22,9 % (trang ẩn lương với người chưa đăng nhập; BD không đăng nhập vào trang nguồn), địa điểm 73,3 % (sau khi nhận "Quận/Huyện …"; phần còn lại là "Not Available"), cấp bậc 98,1 %, kinh nghiệm 94,6 %, ngày đăng 100 %. So VietJobs: lương 71,4 %, địa điểm 98,5 % | Đạt; lương ITviec thấp do nguồn |
| F4 | itviec: 561 phiên bản / 536 tin, 24 tin "đổi" – **cả 24 là đổi giả** do id trùng (mục 2, phát hiện 1). `title` và `company_key` cùng đổi 25 lần, các trường khác đổi theo. topcv: 0 tin đổi vì các đợt crawl không có tin chung (phát hiện 2). Tỉ lệ đổi thật trên dữ liệu này là 0, chưa so được với giả định 5 %/lần của `recrawl_sim`. Bất biến SCD2 (dbt test trong DAG): `<cần xác nhận>` | Pipeline đạt; chưa có dữ liệu đổi thật |
| F5 | Trễ tới khi bronze đủ dòng, gửi cách nhau 30 s, trigger 10 s: `itviec-jobs/2026-11-13` 561 tin – 26 s; `topcv/2026-10-12` 304 tin – 18 s; `topcv/2026-11-15` 27 tin – 16 s; `topcv/2026-11-18` 21 tin – 7 s | Đạt (< 60 s) |
| F6 | Chạy với **stub**: job_skills 21.360 dòng / 874 tin; job_dedup 5 nhóm > 1 tin (10 tin), 2 nhóm khác trang; review_aspects 1.085 dòng / 144 review. Kết quả stub không so được với báo cáo WM. Chờ image bản thật | Chưa đo |
| F7 | mart_skill_demand_monthly 5.600 dòng, mart_company_aspect_sentiment 554 dòng, dim_company có review 144 (mỗi công ty một review trong dữ liệu nhận). Top kỹ năng hiện đến từ stub, chưa có ý nghĩa | Đạt (đường dữ liệu) |

**Ghi chú khi đọc kết quả.** `is_active` = 0 với mọi tin crawl. `silver_job_postings` đánh dấu hết hạn khi
`last_seen_ts` cũ hơn 7 ngày tính theo **giờ chạy**, mà đợt crawl mới nhất là 18/11. Đây là đúng quy tắc khi nạp
dữ liệu cũ, không phải lỗi. Nếu cần phát lại dữ liệu cũ thì mốc nên là `crawl_ts` lớn nhất của nguồn – để lại làm
việc sau.

### Lần 2 – image bản thật

`<cần đo>` – khi có image WM bản thật: `install.sh full` → DAG → `report-real-ingest.sh`, điền F6, F7.

## 7. Rủi ro đã biết

- `absa-scorer --changed-since` lọc theo `crawl_ts` (thời điểm crawl): review crawl từ trước rồi mới gửi vào Kafka sẽ
  bị bỏ sót nếu chạy tăng dần → DAG **không** truyền tham số này cho absa (xử lý lại toàn bộ, review ít). Đề nghị WM đổi
  mốc sang `ingest_ts` của bronze.
- Retention các topic `jobs.raw.*` là 7 ngày: Kafka không phải nơi lưu lâu dài; tái xử lý lịch sử dựa trên bronze.
- Bản `ml` của skill-extractor (PhoBERT CPU, image ~3,4 GB) giới hạn 4 Gi RAM trong DAG; máy 8 GB không nên chạy cùng
  Spark – đo RAM thật ở bước 8.
