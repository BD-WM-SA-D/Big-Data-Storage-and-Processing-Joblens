# Kế hoạch nâng cấp nền tảng JobLens theo domain IT4931 Big Data Storage and Analysis

Trạng thái: **đề xuất, chưa thực thi**. File này là nguồn nội bộ để phân rã việc cần làm, không phải báo cáo môn học; khi đưa vào báo cáo phải viết lại thành văn theo quy tắc R3/R5 của nhóm.

Cập nhật: 2026-11-24.

## 1. Mục tiêu và nguyên tắc chỉ đạo

Mục tiêu không phải là “thêm thật nhiều công nghệ”, mà là làm cho các lựa chọn **Storage – Move – Process – Query/Analyze – Integrate** hiện có được chứng minh đúng bằng mã nguồn, phép đo và khả năng phục hồi. Trọng tâm theo góp ý của giảng viên là **biến đổi dữ liệu**: dữ liệu phải có lịch sử, có giải thích được cách chuẩn hóa, có cổng chất lượng chặn dữ liệu xấu, và có thể replay để đối chiếu.

Nguyên tắc:

1. **Contract trước, code sau.** Mọi cột/bảng/topic mới thuộc hợp đồng được khai báo ở `joblens` trước; repo này không giữ bản sao schema. Đổi tên/xóa field phải hỏi nhóm. Tham khảo `docs/ke-hoach-mo-rong.md:55-60`.
2. **Đúng ngữ nghĩa trước, tối ưu sau.** Sửa idempotency, freshness, schema gate trước khi làm incremental hay thêm dịch vụ mới.
3. **Không bịa số đo.** Chỗ chưa chạy ghi `<cần đo>`; sau mỗi lần demo hoặc chốt số thêm một dòng vào `deploy/compat.md`.
4. **Pin phiên bản.** Không dùng tag `latest`; image dạng `name:version-gitsha` như `deploy/lib.sh:31-35`.
5. **Một artifact, một môn chấm.** Không kéo spider/parser/mô hình/UI của repo khác vào đây; xem `AGENTS.md` và `docs/yeu-cau-mon-hoc.md:150-152`.
6. **Thay đổi có đường lui.** Mọi DDL/DML trên bảng thật phải có bản backup/snapshot, lệnh rollback và một lần thử khôi phục trước khi áp dụng.

## 2. Đối chiếu với yêu cầu môn học

Yêu cầu gốc nằm ở `docs/yeu-cau-mon-hoc.md`:

| Nhóm yêu cầu | Trạng thái hiện tại | Hướng nâng cấp trong kế hoạch này |
|---|---|---|
| Pipeline hoàn chỉnh, Lambda/Kappa, thu thập → xử lý → lưu trữ → trực quan (`docs/yeu-cau-mon-hoc.md:19-22`) | Đã có, ADR Kappa còn trạng thái “Đề xuất” (`docs/adr/0001-lambda-vs-kappa.md:3`) | Điền số đo gold freshness vào ADR `:39`, chốt trạng thái sau khi nhóm duyệt (G0.3) |
| Spark: aggregation, UDF, join, tối ưu, streaming, MLlib (`:34-40`) | Đã có phần lớn; GraphFrames chưa làm (`:163`) | Không tự làm GraphFrames skill graph; hỏi giảng viên, nếu bắt buộc thì đồ thị dữ liệu BD do repo này tạo, không đụng mô hình Web Mining |
| Quality gate trước publish (`:42-47`, `:123-137`) | GX 15 kỳ vọng, nhưng chưa có freshness/volume gate (`spark-jobs/gx_silver.py:60-100`) | P0.3, P0.5 |
| Exactly-once và state (`:39`, `:129`) | Checkpoint + upsert có ở stream cửa sổ (`spark-jobs/stream_province_windows.py:102-126`), nhưng bronze sink là 2 lần append (`spark-jobs/bronze_jobs.py:234-243`) | P0.1, P0.4 |
| Biến đổi dữ liệu + nhiễu (`:64-65`, `:139-152`) | Đã có robust normalization và thí nghiệm nhiễu; SCD2/incremental/T6 còn ⬜ (`:171-173`) | P1.1–P1.5 |
| Giám sát, khôi phục, production (`:133-137`, `:175`) | Grafana/Kafka UI demo; chưa có Prometheus/SLO/backup drill (`docs/ke-hoach-mo-rong.md:18-20`) | P2.1–P2.3 |

## 3. Tổng quan lộ trình

| Giai đoạn | Thời gian ước tính | Mục tiêu | Cổng nghiệm thu |
|---|---:|---|---|
| G0 – Chuẩn bị | 0,5 ngày | Chốt quyết định, baseline, phạm vi | Baseline xanh; câu hỏi contract/giảng viên có người phụ trách |
| G1 – Đúng dữ liệu | 1–2 ngày | Sửa các điểm có thể gây mất/sai dữ liệu mà vẫn báo xanh | Replay 2 lần không trùng; schema lạ bị cách ly; no-new-data làm DAG đỏ |
| G2 – Đúng phân tích | 2–3 ngày | SCD2, incremental gold, grain đúng, maintenance đầy đủ | Có số đo full vs incremental; history không chồng lấp; panel đúng 1 dòng/tỉnh/tháng |
| G3 – Production-like có chừng mực | 1–2 ngày | Alert stream, alert rule, CI, resilience có điều kiện | 0 OOM 24h; alert drill 1/0/0; mọi số mới nằm trong `deploy/compat.md` |

Thứ tự là bắt buộc: **không sang G2 khi G1 chưa xanh**, vì incremental/SCD2 sẽ nhân bản lỗi idempotency và freshness lên nhiều bảng hơn.

## 4. G0 – Chuẩn bị và quyết định

### G0.1 – Baseline phải xanh trước khi sửa

Việc cần làm:

- Chạy trên `JOBLENS_PROFILE=full` (profile lite từng OOM khi dbt, `deploy/trino/values-lite.yaml:2-4`).
- Ghi lại số hiện tại từ `deploy/compat.md:16`: silver 48.095, gold 2.275, smoke synthetic 15 + VietJobs 48.092. Không tự đổi số này.
- Chạy các lệnh kiểm tra ở mục 8; lưu output thật vào báo cáo triển khai, không commit log.

Tiêu chí xong: `pre-commit`, `pytest`, `dbt build`, smoke đều chạy được; mọi lỗi còn tồn tại được ghi thành issue/task trong file này.

### G0.2 – Chốt ba quyết định trước khi đụng code

| Quyết định | Ai chốt | Nếu chưa chốt |
|---|---|---|
| Có đổi hợp đồng ở `joblens` để thêm `silver.job_postings_history`, mart mới, schema `alerts.job_match.v1` không? | Nhóm repo chung | Chỉ làm PoC trong schema `lab.*`; không publish bảng/topic mới |
| Giảng viên có bắt buộc GraphFrames không (`docs/yeu-cau-mon-hoc.md:163`)? | Người phụ trách môn | Giữ giải thích “đã hỏi, chưa có xác nhận”; không tự thêm graph skill của Web Mining |
| Máy chủ có cấp thêm RAM cho 3 broker Kafka / Mongo replica không (`docs/ke-hoach-mo-rong.md:41-43`)? | Người vận hành | Giữ cấu hình 1 broker/1 Mongo; chỉ đo và nêu giới hạn |

### G0.3 – Làm rõ ADR Kappa

- Điền ô độ tươi gold tại `docs/adr/0001-lambda-vs-kappa.md:39` bằng số đo thật sau khi có `gold_freshness_check` (P0.5).
- Cập nhật checkbox `:68-73` theo trạng thái thật; giữ `<người kiểm tra>` nếu chưa có người duyệt.
- Không tự đổi “Đề xuất” thành “Đã chấp nhận” khi chưa có nhóm duyệt.

## 5. G1 – Đúng dữ liệu

### P0.1 – Bronze sink idempotent theo khóa Kafka

**Vấn đề.** `foreachBatch` hiện ghi tốt vào `jobs_raw` bằng `append()` và dòng hỏng vào `ingest_errors` bằng `append()` độc lập (`spark-jobs/bronze_jobs.py:234-243`). Hai commit này không nguyên tử; retry sau lỗi có thể trùng khóa `(kafka_topic, kafka_partition, kafka_offset)`. DDL `:58-79` không có ràng buộc duy nhất. Tài liệu demo lại nói “exactly-once” (`docs/demo.md:88`, `docs/chay-demo.md:163-164`) trong khi bài học nội bộ đã nếu đúng điều kiện cần có idempotent sink (`docs/report/bai-hoc-kinh-nghiem.md:95`).

**Việc cần làm.**

- Trích helper sinh SQL MERGE thuần để kiểm thử không cần Iceberg, đặt trong `spark-jobs/bronze_jobs.py` cạnh `to_bronze()`/`errors()`.
- Trước MERGE, lọc `batch` theo khóa Kafka, giữ bản có `kafka_offset` lớn nhất nếu cùng batch có trùng khóa.
- Thay hai lệnh append bằng hai lệnh `MERGE INTO ... WHEN NOT MATCHED THEN INSERT *`, dùng `USING` là view tạm của batch đã lọc.
- Giữ chế độ `format-version = 2`, `write.merge.mode = merge-on-read` như hiện tại; không đổi partitioning.
- Cập nhật mô tả demo `docs/demo.md:88` và `docs/chay-demo.md:163-164` sau khi code chạy xong: nói rõ checkpoint + MERGE theo khóa Kafka + offset audit, không nói chung chung “upsert idempotent”.

**File dự kiến sửa:** `spark-jobs/bronze_jobs.py`, `tests/test_bronze_jobs.py`, `docs/demo.md`, `docs/chay-demo.md`, `deploy/compat.md`.

**Nghiệm thu.**

- Unit test mới: cho cùng một batch vào hai lần, số dòng trong mỗi bảng đích không đổi.
- Trên cụm, kill/restart job giữa một micro-batch rồi replay: `SELECT kafka_topic, kafka_partition, kafka_offset, count(*) FROM iceberg.bronze.jobs_raw GROUP BY 1,2,3 HAVING count(*) > 1` trả **0 dòng**; làm tương tự cho `ingest_errors`.
- Dòng `deploy/compat.md` mới ghi rõ lần drill và số dòng trước/sau.

**Rollback.** Revert commit và dùng Iceberg time travel về snapshot trước drill; không xóa dữ liệu thật.

### P0.2 – Chặn schema/source lạ ngay khi nạp bronze

**Vấn đề.** `MESSAGE_SCHEMA` có `schema_version` (`spark-jobs/bronze_jobs.py:31-40`) nhưng vòng kiểm tra `:140-154` chỉ xét null. Tin `job_posting.v2` hoặc `source` lạ vẫn vào bronze; GX chỉ phát hiện nguồn lạ sau khi đã vào silver (`spark-jobs/gx_silver.py:66-67`). Rủi ro đã ghi trong `docs/report/rui-ro-trien-khai-san-pham.md:29,79`.

**Việc cần làm.**

- Định nghĩa hằng `ALLOWED_SCHEMA_VERSIONS = {"job_posting.v1"}` và `ALLOWED_SOURCES` lấy cùng danh sách với GX (`topcv`, `itviec`, `vietjobs`, `synthetic`, `spark-jobs/gx_silver.py:37`).
- Trong `to_bronze()`, thêm điều kiện invalid khi `schema_version` không thuộc allow-list hoặc `source` không thuộc allow-list.
- Mở rộng hàm `reason()` để phân biệt `unsupported_schema`, `unsupported_source:<giá trị>`, `missing:...`, `invalid_json`; giữ `value_prefix` để replay.
- Thêm ca kiểm thử trong `tests/test_bronze_jobs.py:110-136`: message v2 và source lạ phải vào `ingest_errors`, không vào bảng tốt.

**Nghiệm thu.**

- `SELECT count(*) FROM iceberg.bronze.jobs_raw WHERE schema_version <> 'job_posting.v1'` = 0.
- Sau khi inject 1 tin v2: số dòng `ingest_errors` tăng đúng 1, reason đúng `unsupported_schema`.

**Không làm.** Không copy schema file từ `joblens` vào repo; không viết parser cho field v2; muốn hỗ trợ v2 thì mở issue contract ở `joblens` trước.

### P0.3 – Freshness gate trước khi publish gold

**Vấn đề.** Khi không có bronze mới, `silver_job_postings` log rồi thoát thành công (`spark-jobs/silver_job_postings.py:234-237`). GX chỉ kiểm `row count >= 1` (`spark-jobs/gx_silver.py:63`), không có kỳ vọng thời gian. dbt source chưa khai báo `freshness` (`dbt/models/sources.yml:3-25`). Hệ quả: 15/15 GX vẫn đạt trên dữ liệu cũ.

**Việc cần làm.**

- Thêm kỳ vọng trên `updated_ts` trong `gx_silver.py`: `now - max(updated_ts)` phải nhỏ hơn ngưỡng chính sách (mặc định 90/180 phút); `last_seen_ts` là event time của nguồn, không dùng để chấm độ tươi pipeline.
- Thêm guard “số dòng mới trong kỳ >= 1” cho mỗi lần chạy DAG; nếu silver không có dòng mới trong N chu kỳ liên tiếp thì task đỏ. N chưa biết → `<cần đo>`.
- Khai báo `freshness` cho source `silver.job_postings` trong `dbt/models/sources.yml`; chạy `dbt source freshness` trước `dbt build`.
- Cập nhật `tests/test_gx_silver.py`: một DataFrame có `updated_ts` cũ phải làm hỏng kỳ vọng freshness, còn `last_seen_ts` cũ nhưng `updated_ts` mới vẫn đạt.

**Nghiệm thu.** Diễn tập “không có dữ liệu mới”: `gx_validate_silver` hoặc `dbt source freshness` trả mã lỗi, `dbt_build_gold` không chạy, snapshot `gold.mart_salary_role_region` không đổi. Ghi giá trị đo được `now - max(updated_ts)` vào `deploy/compat.md` thay cho `<cần đo>`.

### P0.4 – Offset audit, đừng tin lag của Kafka UI

**Vấn đề.** Spark đọc Kafka không set `group.id` (`spark-jobs/bronze_jobs.py:222-231`, `spark-jobs/stream_province_windows.py:215-222`) nên Kafka UI không có consumer lag thật để hiện, dù `deploy/monitoring/install.sh:4` có nhắc “lag của consumer”. ADR cũng yêu cầu theo dõi lag (`docs/adr/0001-lambda-vs-kappa.md:63`).

**Việc cần làm.**

- Tạo bảng nội bộ `bronze.kafka_offset_audit` (không phải contract bảng; tương tự `bronze.ingest_errors`): `topic`, `partition`, `kafka_end_offset`, `bronze_max_offset`, `gap`, `measured_at`.
- Trong `bronze_jobs`, sau mỗi micro-batch, lấy end offset của các topic trong `TOPIC_PATTERN` bằng Kafka admin client hoặc métadata từ source, so với `max(kafka_offset)` hiện có trong bảng theo cặp topic/partition.
- Thêm một Grafana stat “Offset gap” và luật cảnh báo khi `gap > 0` trong 2 chu kỳ liên tiếp.
- Sửa mô tả `deploy/monitoring/install.sh:4` để không hứa lag consumer không tồn tại; nói rõ đây là offset audit do repo tự tính.

**Nghiệm thu.** Sau replay 1.000 tin và đợi job chạy 1 giờ: `max(gap)` = 0; nếu cố tình dừng job và replay, ô Grafana phải đỏ. Không được dùng con số Kafka UI để thay thế.

### P0.5 – DAG có timeout và kiểm tra gold freshness

**Vấn đề.** DAG chạy mỗi giờ, `retries=1`, `max_active_runs=1` nhưng không có `execution_timeout` (`airflow/dags/joblens_pipeline.py:92-96`); một Spark pod treo có thể chặn các run sau.

**Việc cần làm.**

- Thêm `execution_timeout` vào `default_args`, giá trị khởi đầu `<45 phút>` và hiệu chỉnh theo số đo thật.
- Thêm task cuối `gold_freshness_check` sau `[gold, analytics]` (trước/kết thúc maintenance tùy thiết kế) để so sánh `max(month)`/thời điểm cập nhật gold với chu kỳ DAG.
- Giữ `max_active_runs=1`; không chạy hai job Spark cùng lúc trên máy hiện tại (`airflow/dags/joblens_pipeline.py:125-127`).

**Nghiệm thu.** Trong 48h không có run nào vượt timeout; p95 thời gian kết thúc DAG đến lúc gold truy vấn được được ghi vào `deploy/compat.md` và ADR `:39`.

### P0.6 – Snapshot ngày thành một commit

**Vấn đề.** `silver_job_postings.py:276-280` `DELETE` rồi `INSERT` snapshot ngày; nếu pod chết giữa hai lệnh thì ngày đó trống.

**Việc cần làm.** Thay bằng `INSERT OVERWRITE` phân vùng `snapshot_date` hoặc ghi bảng tạm rồi swap; giữ ngữ nghĩa “chạy lại trong ngày thì ghi đè ngày đó” (`silver_job_postings.py:10`). Thêm test đếm số dòng theo ngày.

**Nghiệm thu.** Sau drill kill pod giữa lúc ghi snapshot: `SELECT snapshot_date, count(*) ... GROUP BY 1 HAVING count(*) = 0` trả 0 dòng.

## 6. G2 – Đúng biến đổi và phân tích

### P1.1 – Lịch sử tin SCD loại 2 (T1)

**Điều kiện.** Phải có contract `silver.job_postings_history` ở `joblens` trước (`docs/ke-hoach-mo-rong.md:57-58`). Nếu chưa có: làm PoC trong `lab.*`, không publish.

**Việc cần làm.**

- Chọn cơ chế: khuyến nghị job Spark `spark-jobs/silver_job_history.py` đọc `silver.job_postings_snapshot` (`spark-jobs/silver_job_postings.py:109-117`) và so sánh `content_hash`/`is_active`/lương giữa hai ngày liên tiếp. Lý do: logic Iceberg MERGE đã có trong repo, không phụ thuộc khả năng `dbt snapshot` trên Trino/Iceberg chưa được kiểm chứng.
- Nếu nhóm chọn `dbt snapshot`, phải thử nghiệm trước một ADR nhỏ ghi rõ giới hạn connector Trino-Iceberg; không đổi thiết kế giữa chừng.
- Cột tối thiểu: `job_uid`, các thuộc tính cần theo dõi, `valid_from`, `valid_to`, `is_current`. Contract bổ sung ở `joblens`.
- Thêm task vào DAG sau `silver_job_postings`, trước GX/gold nếu gold sẽ đọc history; nếu gold chưa đọc thì có thể chạy song song nhưng vẫn tuân thủ giới hạn RAM.

**Nghiệm thu.** Một tin đổi lương giữa hai snapshot cho đúng 2 dòng, `valid_to` của dòng cũ bằng `valid_from` của dòng mới, chỉ 1 dòng `is_current = true`, không khoảng thời gian chồng lấp. Ghi ví dụ vào `docs/kien-truc.md` hoặc tài liệu báo cáo nguồn.

### P1.2 – Gold tăng dần và đo được lợi ích (T2)

**Vấn đề.** `dbt/dbt_project.yml:24-28` cho cả hai model `materialized: table`; mỗi giờ dựng lại toàn bộ 48 nghìn tin.

**Việc cần làm.**

- Đổi `mart_salary_role_region` sang `incremental` với `unique_key` là tổ hợp grain thật `(month, category_source, level_code, location_code)`. Lưu ý model hiện tạo cả dòng `ALL` (`dbt/models/gold/mart_salary_role_region.sql:45-54`) nên không được dùng `job_uid` làm unique key.
- Giữ `dim_location` là `table` vì chỉ 34 dòng seed; không “incremental hóa” mọi thứ.
- Thêm cờ chạy full refresh có kiểm soát (`dbt build --full-refresh`) trong runbook.
- Đo trước/sau: thời gian `dbt build`, số byte quét/gin Trino, số snapshot Iceberg. Chưa có số → `<cần đo>`; không viết “nhanh hơn X%” khi chưa đo.

**Nghiệm thu.** Kết quả incremental so với full refresh sai khác 0 dòng/0 giá trị trên cùng đầu vào; thời gian và byte quét được ghi cụ thể vào `benchmarks/` và `deploy/compat.md`.

### P1.3 – Sửa grain của panel Grafana

**Vấn đề.** Ba panel truy vấn `iceberg.gold.mart_salary_role_region` nhưng không lọc `month` (`deploy/monitoring/dashboards/joblens-demo.json:454,506,558`), trong khi mart có một dòng cho mỗi tháng (`dbt/models/gold/mart_salary_role_region.sql:13,45-54`).

**Việc cần làm.**

- Thêm điều kiện `AND m.month = (SELECT max(month) FROM iceberg.gold.mart_salary_role_region)` hoặc cho phép chọn tháng qua biến Grafana.
- Giảm refresh từ `5s` (`:10`) xuống `30s` để giảm tải Trino một node; cân nhắc lại sau khi có seed/phần cứng mạnh hơn.
- Export JSON lại theo hướng dẫn `deploy/monitoring/install.sh:6`; không sửa tay trên Grafana rồi quên đưa về manifest.

**Nghiệm thu.** `count(*) = count(DISTINCT location_code)` cho panel theo tỉnh ở tháng mới nhất; không còn dòng trùng tên tỉnh giữa các tháng. Dashboard refresh 30s, không OOM Trino trong 24h.

### P1.4 – Bảo trì Iceberg cho cả gold

**Vấn đề.** `maintenance_iceberg.py:25-29` chỉ liệt kê bronze và silver; test `:41-45` còn khẳng định vòng loại đó. Gold dựng lại mỗi giờ nên sinh snapshot vô hạn.

**Việc cần làm.**

- Thêm `gold.mart_salary_role_region` và `gold.dim_location` vào danh sách bảo trì, nhưng dùng chính sách riêng: `expire_days=1`, `retain_last=24` (phản ánh 24 lần chạy/ngày). Không dùng chung 7 ngày/10 snapshot của bronze.
- Cập nhật `tests/test_maintenance_iceberg.py`: bỏ khẳng định “chỉ bronze/silver.job_postings”, thay bằng khẳng định không chạm bảng mining (`silver.job_skills`, `silver.job_dedup`, `silver.review_aspects`, `silver.reco_scores`).
- Không bật `remove_orphan_files` khi streaming đang ghi (`maintenance_iceberg.py:13`).

**Nghiệm thu.** Sau 48h, `SELECT count(*) FROM iceberg.gold."mart_salary_role_region$snapshots"` ≤ 24; tổng byte metadata đi ngang thay vì tăng tuyến tính.

### P1.5 – Tỷ giá USD theo ngày đăng (T5)

**Vấn đề.** `normalize.py:23-35` dùng `DEFAULT_USD_VND = 26_000`; `deploy/spark/submit.sh:86` truyền mặc định 26.000 cho mọi job. Mọi tin USD bị ép cùng một tỷ giá.

**Việc cần làm.**

- Thêm fixture tỷ giá có nguồn và ngày lấy, ví dụ `spark-jobs/exchange_rates.csv`, dùng chung cơ chế với `dbt/seeds/locations_2025.csv` (`normalize.py:90-115`). Không crawl tỷ giá từ web.
- Trong `normalize_payload()`, nếu có `posted_date` thì tra tỷ giá theo ngày gần nhất <= ngày đăng; nếu không có thì fallback `JOBLENS_USD_VND` và log cảnh báo.
- Không thêm cột contract mới nếu chưa được `joblens` chấp thuận; chỉ đổi giá trị `salary_min_vnd`/`salary_max_vnd` đã có.
- Thêm test cho ngày có tỷ giá, ngày thiếu, và `posted_date = null`.

**Nghiệm thu.** Tin `"1,000 - 2,000 USD"` ngày có tỷ giá 25.400 cho ra 25,4–50,8 triệu; với ngày thiếu thì fallback rõ ràng và có log. Nếu đổi tỷ giá lịch sử, replay lát liên quan vào nhánh riêng rồi GX mới publish.

### P1.6 – Golden set cho quy tắc chuẩn hóa đã sửa

**Vấn đề.** Kết quả robust ~98% đo trên bộ nhiễu mô phỏng 3 kiểu lỗi gõ (`docs/thi-nghiem-nhieu.md:274-278,289-297`); không phải bảo chứng cho mọi nguồn thật.

**Việc cần làm.** Tập hợp các chuỗi đã từng gây tranh cãi (`"hải châu"`, `"văn giang"`, `"0.0 triệu"`, `"200 - 500 triệu"`, `"ho chi minh city"`) thành golden set trong `tests/test_normalize.py`; mỗi ca có kỳ vọng rõ “sửa đúng / cố ý giữ null / cố ý loại”. Gắn lý do quyết định vào `docs/thi-nghiem-nhieu.md`.

**Nghiệm thu.** Mọi thay đổi sau này của `normalize.py` phải pass golden set; nếu có ca mới làm thay đổi kết quả cũ thì phải có replay/so sánh trước khi publish (liên kết P1.1/P1.5).

## 7. G3 – Production-like có chừng mực

### P2.1 – Job ghép cảnh báo việc làm

**Điều kiện.** Schema `alerts.job_match.v1` phải có ở `joblens` trước khi emit chính thức. Topic đã khai báo ở repo này (`deploy/kafka/topics.yaml:78-102`).

**Việc cần làm.**

- Thêm `spark-jobs/job_alerts.py`: đọc compacted `app.alert_subscriptions.v1`, join với `jobs.raw.*.v1` theo ngữ cảnh đăng ký (ví dụ tỉnh × cấp bậc × nhóm nghề), ghi `alerts.job_match.v1` theo `user_id`.
- Dùng state/checkpoint riêng; alert id ổn định để sink idempotent khi replay.
- Thêm smoke script gửi 1 subscription + 1 tin khớp + 1 tin không khớp, rồi replay lại.
- Không làm API/UI thông báo; PTTKHT là nơi consume topic.

**Nghiệm thu.** 1 cặp khớp → đúng 1 alert; tin không khớp → 0 alert; replay → 0 alert trùng.

### P2.2 – Prometheus/Alertmanager tối thiểu

**Việc cần làm.**

- Khai báo qua Helm values/Secret (không nhập tay trên UI), 4 luật: dữ liệu cũ hơn ngưỡng, offset gap > 0, GX/DAG failure, Trino/Spark OOMKilled.
- Gộp log GX thành một dòng/bản ghi có thể query được thay vì chỉ JSON S3 (`spark-jobs/gx_silver.py:168-190`): ví dụ bảng nội bộ `bronze.gx_runs` với `run_id`, `failed_count`, `results_uri`.
- Gửi cảnh báo nội bộ tới kênh nhóm; token nằm trong Secret, không commit.
- Không đặt tên người nhận cụ thể; để `<người kiểm tra>`/`kênh nhóm`.

**Nghiệm thu.** Khi inject lỗi GX, alert tới trong `<cần đo>` phút; khi dừng bronze job, alert offset gap tới; không có secret nào lọt vào git (pre-commit gitleaks pass).

### P2.3 – Resilience có điều kiện

Chỉ làm sau khi có số đo RAM:

- Kafka 3 broker, `replication.factor=3`, `min.insync.replicas=2`, producer `acks=all` (`docs/ke-hoach-mo-rong.md:41`): ước tính +1–2 GB RAM; demo tắt 1 broker rồi đếm dòng trước/sau.
- MongoDB replica set 3 thành viên (`:43`): đo write/read concern; không thay mặc định khi máy còn 12–14 GB budget.
- Spark dynamic allocation và Trino resource group (`:44-46`): chỉ khi có từ 2 worker trở lên; nếu không, ghi rõ giới hạn.

**Nghiệm thu.** Có bảng RAM trước/sau, số dòng trước/sau khi hỏng 1 thành viên, và dòng mới trong `deploy/compat.md`. Nếu không đủ RAM, giữ cấu hình hiện tại và ghi “chưa áp dụng, lý do: RAM”.

### P2.4 – CI tối thiểu

**Việc cần làm.** Thêm workflow chạy `pre-commit run --all-files`, `uv run pytest -q`, kiểm tra manifest/KafkaTopic bằng diff file YAML, và `dbt parse`. `dbt build` chỉ bật nếu có runner có Trino; nếu không, ghi rõ lý do. Không dùng secret thật trong CI.

**Nghiệm thu.** Mỗi PR ở repo này có 3 check đầu xanh; không artifact dữ liệu/log được upload.

## 8. Danh mục file dự kiến thay đổi

| Việc | File tạo/sửa |
|---|---|
| P0.1 | `spark-jobs/bronze_jobs.py`, `tests/test_bronze_jobs.py`, `docs/demo.md`, `docs/chay-demo.md` |
| P0.2 | `spark-jobs/bronze_jobs.py`, `tests/test_bronze_jobs.py` |
| P0.3 | `spark-jobs/gx_silver.py`, `dbt/models/sources.yml`, `tests/test_gx_silver.py`, `airflow/dags/joblens_pipeline.py` |
| P0.4 | `spark-jobs/bronze_jobs.py`, `deploy/monitoring/dashboards/joblens-demo.json`, `deploy/monitoring/install.sh` |
| P0.5 | `airflow/dags/joblens_pipeline.py`, `docs/adr/0001-lambda-vs-kappa.md` |
| P0.6 | `spark-jobs/silver_job_postings.py`, `tests/test_silver_job_postings.py` |
| P1.1 | `joblens` contract trước, sau đó `spark-jobs/silver_job_history.py`, `airflow/dags/joblens_pipeline.py`, `tests/` |
| P1.2 | `dbt/dbt_project.yml`, `dbt/models/gold/mart_salary_role_region.sql`, `benchmarks/` |
| P1.3 | `deploy/monitoring/dashboards/joblens-demo.json` |
| P1.4 | `spark-jobs/maintenance_iceberg.py`, `tests/test_maintenance_iceberg.py` |
| P1.5 | `spark-jobs/normalize.py`, `spark-jobs/exchange_rates.csv`, `tests/test_normalize.py` |
| P1.6 | `tests/test_normalize.py`, `docs/thi-nghiem-nhieu.md` |
| P2.1 | `joblens` schema trước, sau đó `spark-jobs/job_alerts.py`, `deploy/data/smoke-alerts.sh`, `tests/` |
| P2.2 | `deploy/monitoring/` (Helm values/rules), `spark-jobs/gx_silver.py`, `tests/` |
| P2.3 | `deploy/kafka/`, `deploy/mongodb/`, `deploy/spark/`, `docs/ke-hoach-mo-rong.md` |
| P2.4 | `.github/workflows/` |

Mọi thay đổi chạy đều phải cập nhật `deploy/compat.md`; thay đổi contract thì nhắc nhóm chat theo quy tắc repo.

## 9. Trình tự kiểm chứng sau mỗi giai đoạn

Chạy từ gốc repo, với `JOBLENS_PROFILE=full`:

```bash
uv run pre-commit run --all-files
uv run pytest -q
bash deploy/smoke-e2e.sh
bash scripts/dbt.sh build
```

Truy vấn kiểm chứng tối thiểu:

```sql
-- Không trùng khóa Kafka (P0.1)
SELECT kafka_topic, kafka_partition, kafka_offset, count(*) n
FROM iceberg.bronze.jobs_raw
GROUP BY 1, 2, 3 HAVING count(*) > 1;

-- Không có schema lạ (P0.2)
SELECT count(*) FROM iceberg.bronze.jobs_raw WHERE schema_version <> 'job_posting.v1';

-- Độ tươi silver (P0.3)
SELECT now() - max(last_seen_ts) AS age FROM iceberg.silver.job_postings;

-- Không có ngày snapshot trống (P0.6)
SELECT snapshot_date, count(*) n
FROM iceberg.silver.job_postings_snapshot
GROUP BY 1 HAVING count(*) = 0;

-- Số snapshot gold sau maintenance (P1.4)
SELECT count(*) FROM iceberg.gold."mart_salary_role_region$snapshots";

-- Grain panel đúng tháng (P1.3)
SELECT count(*) total_rows, count(DISTINCT location_code) distinct_codes
FROM iceberg.gold.mart_salary_role_region
WHERE month = (SELECT max(month) FROM iceberg.gold.mart_salary_role_region);
```

Kết quả mong đợi đã nêu ở từng task. Không thay thế bằng “nhìn thấy dashboard xanh”; phải có số và dòng `deploy/compat.md`.

## 10. Rủi ro và cách xử lý chung

| Rủi ro | Phòng tránh |
|---|---|
| MERGE chạy chậm trên bronze lớn | Giữ partitioning hiện tại; đo trước/sau; nếu cần chỉ rewrite lát mới; không tắt compaction |
| Incremental gold khác full refresh | Chạy song song trong `lab.*`, so sánh 0 diff trước khi publish |
| PoC SCD2 làm bẩn bảng thật | Chỉ `lab.*` khi chưa có contract; không ghi `silver.job_postings_history` khi chưa có schema ở `joblens` |
| OOM Trino/Spark | Dùng profile full; giữ 1 luồng dbt khi cần; `execution_timeout`; không bật nhiều dịch vụ cùng lúc |
| Sửa tay trên cụm | Mọi thay đổi phải đưa ngược vào Helm/manifest; dashboard export JSON lại |
| Secret lọt repo | pre-commit gitleaks; Secrets qua Helm; không log token |
| Contract đổi gây vỡ repo khác | Thêm field không bắt buộc trước; đổi tên/xóa phải hỏi nhóm; mở issue `cross-repo` |

## 11. Định nghĩa “xong” của toàn kế hoạch

Kế hoạch chỉ coi là hoàn tất khi:

1. G1 xanh: replay/idempotency, schema gate, freshness gate, offset audit, snapshot atomic đều có test và drill.
2. G2 xanh: history/incremental/maintenance/FX có số thật; `<cần đo>` liên quan được điền hoặc vẫn nêu rõ lý do chưa đo.
3. G3 xanh hoặc có lý do bằng số để không làm (RAM, contract, xác nhận giảng viên).
4. `deploy/compat.md` có dòng mới cho từng lần chốt số liệu; `docs/adr/0001-lambda-vs-kappa.md` có số gold freshness và trạng thái đúng quy trình duyệt.
5. `uv run pre-commit run --all-files`, `uv run pytest -q`, smoke và dbt build chạy thành công trên cấu hình đã ghi.
6. Không có file cấm, dữ liệu crawl, log thật, model, secret hoặc output notebook được commit.

## 12. Việc không thuộc repo này

- Web Mining: spider, parser HTML, dedup mờ, IE/ABSA/IR/RecSys, model files, `silver.job_skills`, `silver.job_dedup`, `silver.review_aspects`, `silver.reco_scores`, GraphFrames skill graph. Repo này chỉ đọc bảng đã chuẩn hóa hoặc gọi image theo tag cố định.
- PTTKHT: API/UI, UML/UC, luồng gửi thông báo tới người dùng. Repo này chỉ ghi `alerts.job_match.v1`.
- `joblens`: schema message, contract bảng, glossary, versions, fixtures. Không tạo bản sao schema ở đây.

Khi cần việc ở repo khác: mở issue nhãn `cross-repo`, không tự sửa. Khi contract thay đổi: nhắn nhóm chat trước khi merge code.
