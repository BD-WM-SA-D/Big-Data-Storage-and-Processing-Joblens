# Rủi ro khi triển khai JobLens thành sản phẩm thực tế (ưu tiên biến đổi dữ liệu)

Tài liệu này là nguồn nội bộ để theo dõi các lỗi có thể xảy ra khi đưa nền tảng IT4931 từ mô hình demo/k3d lên môi trường chạy liên tục. Trọng tâm là **biến đổi dữ liệu** theo góp ý của giảng viên (`docs/ke-hoach-mo-rong.md:3-5`), nhưng vẫn bao gồm hạ tầng, điều phối, chất lượng và vận hành vì một lỗi nhỏ ở các phần đó cũng làm sai kết quả phân tích.

Nguyên tắc ghi số liệu: chỉ dùng số đã có trong `deploy/compat.md`, `docs/report/bai-hoc-kinh-nghiem.md`, `docs/thi-nghiem-nhieu.md` và log đã kiểm chứng. Chỗ chưa đo ghi `<cần đo>`; không suy diễn thành số đo. Mức “cao/trung bình/thấp” ở bảng dưới là **đánh giá thiết kế**, không phải kết quả benchmark.

---

## 1. Bối cảnh và phạm vi đánh giá

Luồng hiện tại (`docs/kien-truc.md:16-58`, `airflow/dags/joblens_pipeline.py:1-23`):

1. Kafka `jobs.raw.*.v1` → Spark `bronze_jobs` → `iceberg.bronze.jobs_raw` (append-only).
2. `silver_job_postings` đọc phần bronze mới theo mốc `joblens.bronze_watermark`, chuẩn hóa bằng `normalize.py`, `MERGE INTO` theo `job_uid`, ghi snapshot ngày.
3. GX kiểm tra silver; hỏng thì dừng trước gold.
4. dbt/Trino dựng `gold.dim_location` và `gold.mart_salary_role_region`; Spark analytics ghi MongoDB; `maintenance_iceberg` gộp file và dọn snapshot.
5. `stream_province_windows` chạy ngoài DAG, dùng event time `crawl_ts` + watermark và upsert MongoDB.

Số liệu đã chốt gần nhất: bản `b4db15d`, profile `full`, silver 48.095 dòng, dbt `PASS=15 ERROR=0`, gold 2.275 dòng, smoke synthetic 15 dòng + VietJobs 48.092 dòng (`deploy/compat.md:16`). Các lỗi thật đã gặp khi bootstrap lại: khóa S3 bị xoay, Trino rollout chưa có container Ready, tải chart Airflow từ `archive.apache.org` timeout, Helm release kẹt `pending-install`, và dbt `OOMKilled` với profile `lite` (`deploy/compat.md:16`, `docs/report/bai-hoc-kinh-nghiem.md:152-159`).

Phạm vi **không** đánh giá ở đây: spider/parser/mô hình IE-ABSA-IR/RecSys của repo Web Mining, API/UI của repo PTTKHT, và việc sửa schema message/contract bảng ở repo `joblens`. Khi phát hiện lỗi thuộc các repo đó, cần mở issue `cross-repo`, không tự sửa bản sao ở đây.

---

## 2. Bảng tổng hợp rủi ro

| ID | Rủi ro | Nhóm | Tác động | Khả năng | Phát hiện sớm | Ưu tiên |
|---|---|---|---|---|---|---|
| R01 | Schema message đổi nhưng bronze vẫn chấp nhận | Biến đổi đầu vào | Dòng bị cách ly hoặc trường null âm thầm | Cao | Số dòng `bronze.ingest_errors`, phân bố `schema_version` | P0 |
| R02 | Mốc watermark của silver đi trước khi MERGE/commit xong | Biến đổi | Mất vĩnh viễn dòng mới | Trung bình | So sánh `max(ingest_ts)` bronze/silver, audit log | P0 |
| R03 | Quy tắc chuẩn hóa thay đổi nhưng không replay | Biến đổi | Kết quả lịch sử không nhất quán với code hiện tại | Cao | Hash phiên bản rule trong run metadata | P0 |
| R04 | Tỷ giá USD cố định lệch thực tế | Biến đổi | Sai hệ thống mặt bằng lương theo thời gian | Cao khi có nhiều tin USD | Tỷ lệ `salary_currency_original = 'USD'`, đối chiếu nguồn tỷ giá | P0 |
| R05 | Địa điểm/lương bị sửa sai do heuristics | Biến đổi | Sai số tin và lương theo tỉnh | Trung bình | `lab.clean_diffs`, GX theo nguồn | P1 |
| R06 | GX chạy trên toàn bảng nên lỗi batch mới bị che | Chất lượng | Dữ liệu xấu vẫn vào gold | Cao | Kết quả GX theo batch/ngày | P0 |
| R07 | dbt dựng lại toàn bộ mỗi giờ | Biến đổi/hiệu năng | Thời gian chạy và RAM tăng theo dữ liệu | Cao | `dbt build` duration, Trino memory | P1 |
| R08 | Trino OOM/không Ready sau rollout | Hạ tầng | `Connection refused`, dbt đỏ | Cao | `Last State`, readiness của coordinator | P0 |
| R09 | Khóa S3 xoay nhưng credential catalog/secret cũ | Hạ tầng | `InvalidAccessKeyId`, mọi ghi Iceberg hỏng | Trung bình | 403 S3, `storage-credential` warehouse | P0 |
| R10 | `failOnDataLoss=false` che mất offset | Luồng | Mất tin không báo lỗi | Trung bình | Kafka lag, khoảng cách offset, cảnh báo mất dữ liệu | P1 |
| R11 | Watermark của stream không khớp độ trễ thực | Luồng | Tin trễ bị bỏ, số cửa sổ thiếu | Cao | `numRowsDroppedByWatermark` | P1 |
| R12 | Snapshot ngày `DELETE` rồi `INSERT` không nguyên tử | Biến đổi | Thiếu cả ngày snapshot nếu lỗi giữa chừng | Trung bình | Số dòng snapshot theo ngày | P0 |
| R13 | Đánh dấu `is_active=false` sai khi nguồn ngừng gửi | Biến đổi | Tin hợp lệ biến mất khỏi gold | Trung bình | Tỷ lệ active theo nguồn, độ trễ nguồn | P1 |
| R14 | Giờ/dev/prod lệch runtime, Python, image | Triển khai | Code chạy local nhưng lỗi trong pod | Trung bình | Tag image `version-gitsha`, smoke trong cụm | P1 |
| R15 | Chart/artifact ngoài mạng không tải được | Triển khai | Bootstrap dừng giữa chừng | Trung bình | Mirror nội bộ + checksum | P1 |
| R16 | Helm release kẹt hoặc sửa tay trên cụm | Vận hành | Manifest không phản ánh thực tế | Trung bình | `helm history`, diff manifest | P1 |
| R17 | Không có Prometheus/SLO/backup thử khôi phục | Vận hành | Phát hiện muộn, không phục hồi được | Cao | Luật cảnh báo, drill khôi phục | P0 |
| R18 | Dirty tree tạo tag image không tái lập được | Quản trị | Không truy lại đúng code đã chạy | Trung bình | CI chặn `git diff` | P1 |
| R19 | Fixtures/uv cache là nguồn dữ liệu của smoke | Triển khai | Smoke đỏ trên máy prod sạch | Cao | Đóng gói fixtures theo bản | P2 |
| R20 | Thiếu contract đổi tên/xóa cột | Quản trị | Pipeline vỡ sau khi repo `joblens` đổi schema | Thấp | Contract test trong CI | P1 |

---

## 3. Rủi ro biến đổi dữ liệu

### 3.1. R01 – Schema message đổi nhưng vẫn đi vào bronze

**Dẫn chứng.** `spark-jobs/bronze_jobs.py:31-40` chỉ khai báo sáu trường ở lớp ngoài; `payload_json` được lấy bằng `get_json_object` và giữ nguyên dạng chuỗi (`:136`). `to_bronze` dùng `from_json` với chế độ mặc định của Spark, rồi chỉ kiểm tra các cột bắt buộc khác null (`:112-154`). `schema_version` được lưu (`:132`) nhưng không có nhánh từ chối giá trị lạ. Dòng hỏng đi vào `bronze.ingest_errors` với lý do `invalid_json` hoặc `missing:...` (`:81-107`, `:157-167`).

**Kịch bản sản phẩm.** Repo Web Mining bắt đầu gửi `job_posting.v2` hoặc đổi kiểu `crawl_ts` sang epoch number. Spark vẫn ghi một phần dòng vào bronze; silver `F.to_timestamp` ở `bronze_jobs.py:133` có thể trả null, UDF `normalize_payload` nhận payload mới và trả giá trị mặc định. GX chỉ thấy tỉ lệ thiếu tăng nhẹ nên có thể vẫn đạt ngưỡng 90%/50%.

**Cách xử lý khi gặp.**

1. Truy vấn phân bố theo ngày, nguồn, phiên bản:

   ```sql
   SELECT kafka_topic, schema_version, count(*) n
   FROM iceberg.bronze.jobs_raw
   WHERE ingest_ts > current_timestamp - INTERVAL '1' DAY
   GROUP BY 1, 2 ORDER BY n DESC;

   SELECT reason, count(*) n
   FROM iceberg.bronze.ingest_errors
   WHERE ingest_ts > current_timestamp - INTERVAL '1' DAY
   GROUP BY 1 ORDER BY n DESC;
   ```

2. Nếu có dòng `schema_version` lạ: tạm dừng producer nguồn đó, giữ payload trong `ingest_errors` để replay, không xóa bronze.
3. Nếu contract v2 chỉ thêm trường không bắt buộc: cập nhật producer trước, rồi cân nhắc mở rộng parser ở repo này. Nếu đổi tên/xóa trường: **báo người dùng để hỏi nhóm trong repo `joblens`**, không tự chế bản sao schema.

**Phòng tránh.** Thêm bước validate message theo contract trước khi `from_json`, hoặc ít nhất một biểu thức chặn `schema_version NOT IN ('job_posting.v1')` để đưa vào `ingest_errors` với lý do `unsupported_schema`. Đặt cảnh báo khi tỉ lệ `ingest_errors` vượt ngưỡng theo nguồn; ghi phiên bản contract vào `deploy/compat.md` mỗi lần đổi.

### 3.2. R02 – Mốc bronze watermark không nguyên tử với MERGE

**Dẫn chứng.** `spark-jobs/silver_job_postings.py:226-263` đọc thuộc tính `joblens.bronze_watermark`, lọc `ingest_ts > mark`, lấy `new_mark = max(ingest_ts)`, thực hiện `MERGE INTO` (`:185-195`, `:241`) rồi mới `ALTER TABLE ... SET TBLPROPERTIES` (`:260-262`). Ba bước này là ba commit độc lập.

**Kịch bản sản phẩm.**

- MERGE commit thành công rồi pod chết trước khi `ALTER`: lần sau đọc lại cùng một lát bronze. MERGE theo `job_uid` nên phần lớn không trùng, nhưng `first_seen_ts`, `last_seen_ts`, `updated_ts` và snapshot có thể bị ghi lại với ngữ cảnh thời gian mới.
- `ALTER` commit rồi MERGE chưa kịp commit hoặc bị rollback: mốc đã đi qua, dòng mới không bao giờ được xử lý. Đây là chế độ mất dữ liệu nguy hiểm nhất vì GX vẫn đạt trên dữ liệu cũ.

**Cách xử lý khi gặp.** So sánh `max(ingest_ts)` ở bronze và mốc trong thuộc tính bảng, đồng thời xem snapshot Iceberg gần nhất. Nếu phát hiện khoảng trống, chạy lại có kiểm soát với lát cũ hơn mốc (`--full` là phương án cuối cùng vì tốn tài nguyên). Sau khi sửa, ghi lý do vào `deploy/compat.md`.

**Phòng tránh.** Ghi mốc ở trạng thái `pending` kèm batch id, chỉ đánh dấu `committed` sau khi MERGE và GX cùng thành công; lúc đọc thì lùi lại một cửa sổ an toàn. Về dài hạn, dùng chính snapshot Iceberg/changelog làm nguồn mốc thay vì thuộc tính tự quản. Thêm audit table `joblens.silver_runs` với `mark_before`, `mark_after`, `rows_merged`, `status`.

### 3.3. R03 – Rule chuẩn hóa đổi nhưng không replay

**Dẫn chứng.** `normalize.py` tập trung mọi quy tắc lương, địa điểm, kinh nghiệm, cấp bậc (`:231-296`, `:178-200`, `:300-336`, `:366-395`). `silver_job_postings.py:68-82` gọi UDF này. Lịch sử sửa rule có thật: `"0.0 triệu"` được đổi thành thỏa thuận, fuzzy địa điểm bị thu hẹp sau khi phát hiện đoán nhầm huyện thành tỉnh (`docs/report/bai-hoc-kinh-nghiem.md:12-30`, `docs/thi-nghiem-nhieu.md:261-288`).

**Kịch bản sản phẩm.** Rule mới chỉ áp cho dòng bronze mới vì watermark đã đi qua. Gold vì thế trộn hai thế hệ chuẩn hóa: cùng một tin cũ vẫn giữ lương sai/địa điểm cũ. Không có cột `rule_version` trong schema hiện tại (`silver_job_postings.py:34-40`), nên không truy vấn được dòng nào dùng rule nào.

**Cách xử lý khi gặp.** Xác định khoảng thời gian đổi rule, replay vào nhánh Iceberg riêng theo Kappa/T3 (`docs/ke-hoach-mo-rong.md:28`), chạy GX trên nhánh mới, so sánh số dòng khác nhau rồi mới publish. Không ghi đè trực tiếp bảng đang phục vụ dashboard.

**Phòng tránh.** Đưa hash của `normalize.py` + file `dbt/seeds/locations_2025.csv` + tỷ giá vào metadata mỗi lần chạy. Khi hash đổi thì CI bắt buộc có bản replay/so sánh. Nếu muốn thêm cột contract, thực hiện ở repo `joblens` trước.

### 3.4. R04 – Lương USD quy đổi theo tỷ giá cố định

**Dẫn chứng.** `normalize.py:23-35` đặt `DEFAULT_USD_VND = 26_000`; `:259-263` dùng `JOBLENS_USD_VND` nếu có, nếu không dùng giá trị mặc định. `deploy/spark/submit.sh:86` truyền mặc định `26000` cho mọi job. `docs/ke-hoach-mo-rong.md:15` đã liệt kê đây là khoảng trống, hướng T5 ở `:30`.

**Kịch bản sản phẩm.** Tin đăng khi tỷ giá thực 25.400 nhưng pipeline dùng 26.000; sai ~2,4% riêng phần lương USD. Nếu tỷ giá thay đổi mạnh theo thời gian, xu hướng lương theo tháng trong gold bị nhiễu. Mặc định dev dễ bị mang sang prod vì không ai set biến môi trường.

**Cách xử lý khi gặp.** Đếm tin có `salary_currency_original = 'USD'` theo tháng; nếu tỷ lệ đáng kể, ghi run mới với `JOBLENS_USD_VND` đúng ngày và replay lát bị ảnh hưởng. Ghi rõ ngày lấy tỷ giá vào `deploy/compat.md`.

**Phòng tránh.** Triển khai join “as-of” theo `posted_date` với bảng tỷ giá fixture có nguồn và ngày lấy (T5). Trước khi có T5, bắt buộc set `JOBLENS_USD_VND` từ cấu hình môi trường và làm preflight in ra giá trị đang dùng; cấm dùng mặc định trong prod.

### 3.5. R05 – Sửa sai địa điểm, lương do heuristics quá mạnh hoặc quá yếu

**Dẫn chứng.** `normalize.py:164-175` chỉ nhận lỗi gõ thiếu/lặp/đảo ký tự kề nhau, không nhận thay ký tự; `:291-296` loại cả khoảng lương nếu một đầu vượt `SALARY_MAX_VALID = 300_000_000`. `docs/thi-nghiem-nhieu.md:261-288` ghi lại trường hợp `hải châu` suýt bị đoán thành Lai Châu và 13 tin `200 - 500 triệu` bị coi là ngoại lai; đây là **quyết định cần người kiểm tra xác nhận**, không phải sự thật đã được chấp thuận.

**Kịch bản sản phẩm.** Nguồn mới dùng tên huyện/thị xã hoặc viết lương năm dạng không có dấu `/`; rule hiện tại có thể để null (giảm độ phủ) hoặc gán sai mã tỉnh khác (sai cả số tin lẫn mức lương). Ngược lại, nới fuzzy để “sửa thêm” sẽ tái diễn lỗi đoán nhầm.

**Cách xử lý khi gặp.** Dùng `iceberg.lab.clean_diffs` và truy vấn các chuỗi chưa ánh xạ (`scripts/check-locations.sql` theo `docs/report/bai-hoc-kinh-nghiem.md:26-28`) để phân loại: alias thật, tên huyện, nước ngoài hay lỗi gõ. Thêm alias theo tần suất; không nới kiểu sai khác nếu chưa có kiểm thử hồi quy.

**Phòng tránh.** Giữ danh mục 34 tỉnh là một nguồn duy nhất (`dbt/seeds/locations_2025.csv`) cho Spark và dbt (`normalize.py:90-115`, `dbt/models/gold/dim_location.sql:3-8`). Mỗi lần đổi alias/lương phải thêm ca kiểm thử trong `tests/test_normalize.py`; khi đổi contract danh mục báo repo `joblens`. Ghi rõ đánh đổi “không sửa lỗi thay ký tự” trong báo cáo môn.

### 3.6. R06 – Cổng GX không đại diện cho batch mới

**Dẫn chứng.** `spark-jobs/gx_silver.py:60-104` chạy 15 kỳ vọng trên toàn bảng silver; ngưỡng độ phủ là 90% địa điểm và 50% lương (`:37-39`). Kết quả ghi lên S3 (`:168-190`); có kỳ vọng hỏng thì trả mã lỗi khi không dùng `--no-fail` (`:193-222`). `dbt/models/sources.yml:12-24` chỉ kiểm vài cột ở nguồn silver.

**Kịch bản sản phẩm.** Một batch mới 1.000 tin với 0% địa điểm vào bảng 48.095 tin cũ: tỉ lệ toàn bảng vẫn ~98,5%, GX đạt, gold được xây lại với dữ liệu sai. Tương tự, `--no-fail` nếu được dùng trong runbook sản xuất sẽ biến cổng thành báo cáo chỉ đọc.

**Cách xử lý khi gặp.** Chạy GX riêng cho lát vừa MERGE (`last_seen_ts`/`updated_ts` trong khoảng chạy) hoặc lưu kết quả theo batch id. Nếu GX toàn bảng đạt nhưng batch mới có tỉ lệ lỗi cao, dừng publish gold và replay batch đó.

**Phòng tránh.** Thêm kỳ vọng theo phân nhóm nguồn và theo ngày; cảnh báo khi `unexpected_percent` tăng dần dù chưa vượt ngưỡng. Cấm `--no-fail` trong DAG; chỉ cho phép khi chạy tay để điều tra và phải ghi log. Lưu JSON kết quả kèm `mark_before/mark_after` để đối chiếu với R02.

### 3.7. R07 – Gold dựng lại toàn bộ và sai lệch ngữ nghĩa khi đọc

**Dẫn chứng.** `dbt/dbt_project.yml:24-28` đặt `+materialized: table` cho cả hai model. `mart_salary_role_region.sql:13` chọn tháng từ `posted_date`, nếu null thì lấy `first_seen_ts`; `:16` gán `unknown` cho tin không có địa điểm; `:23` loại `source = 'synthetic'`; `:38-43` đếm distinct `job_uid`, dùng trung điểm lương và `approx_percentile`; `:45-54` tạo các dòng tổng `grouping sets`; `:63-65` chỉ trả phân vị khi có ít nhất 5 tin có lương. Test `row_count_equals 34` và `relationships_or_all` nằm ở `dbt/models/gold/schema.yml:4-31`.

**Kịch bản sản phẩm.** Ở quy mô lớn hơn, mỗi giờ dbt quét toàn silver và tạo lại bảng; Trino một coordinator dễ OOM (đã xảy ra ở 48 nghìn tin với profile lite, `docs/report/bai-hoc-kinh-nghiem.md:152-159`). Ngoài ra, nếu nguồn mới gửi `posted_date` không chính xác, dòng tin rơi sang tháng khác dù `first_seen_ts` đúng; nếu `source` viết hoa, tin synthetic lọt vào mart.

**Cách xử lý khi gặp.** So sánh `row_count` và tổng `job_count` giữa silver và mart; kiểm tra riêng tin `synthetic`, `unknown`, và tin có `posted_date` khác tháng `first_seen_ts`. Với OOM: dùng profile full, giảm `DBT_THREADS`, chờ Trino Ready rồi retry (`scripts/dbt.sh:12`, `:35-58`).

**Phòng tránh.** Chuyển sang `incremental` theo `month`/changelog (T2, `docs/ke-hoach-mo-rong.md:27`) và đo thời gian/byte quét trước sau bằng số liệu thật trong `benchmarks/`. Chuẩn hóa `source` thành danh mục được GX kiểm tra. Viếtrõ định nghĩa “tháng của tin” trong contract gold; nếu đổi định nghĩa phải báo repo `joblens`.

### 3.8. R12 – Snapshot ngày có cửa hở giữa DELETE và INSERT

**Dẫn chứng.** `silver_job_postings.py:265-280` thực hiện `DELETE FROM snapshot WHERE snapshot_date = ...` rồi `INSERT INTO ... SELECT ...` thành hai lệnh. Chạy lại trong ngày thì ghi đè ngày đó theo thiết kế (`:10`).

**Kịch bản sản phẩm.** Pod bị giết sau `DELETE`: dashboard lịch sử mất một ngày, GX và dbt vẫn có thể đạt vì bảng chính `silver.job_postings` không bị gì.

**Cách xử lý khi gặp.** Đếm dòng snapshot theo `snapshot_date`; nếu ngày hôm trước thiếu, chạy lại `silver_job_postings --snapshot-date <ngày>` (có thể giới hạn lát bronze bằng mốc đã lưu).

**Phòng tránh.** Dùng một lệnh `INSERT OVERWRITE` theo phân vùng hoặc ghi vào bảng tạm rồi swap metadata; thêm kiểm tra “mỗi ngày có đúng số tin active+inactive” trước khi báo task thành công.

### 3.9. R13 – Tin bị đánh dấu hết hạn sai

**Dẫn chứng.** `silver_job_postings.py:265-269` đặt `is_active=false` khi quá `deadline` hoặc `last_seen_ts` cũ hơn `--stale-days` (mặc định 7 ngày). `mart_salary_role_region.sql` dùng chung bảng silver này để đếm số tin.

**Kịch bản sản phẩm.** Crawler của một nguồn ngừng 8 ngày do lỗi ở repo Web Mining; tin vẫn còn trên web nhưng pipeline coi là hết hạn, mart giảm mạnh. Ngược lại, tin đã đóng nhưng crawler vẫn gửi lại với `crawl_ts` mới có thể được giữ active.

**Cách xử lý khi gặp.** Xem `count(*)`, `sum(is_active)` theo `source` và ngày (`silver_job_postings.py:281-284` đã log). Đối chiếu với độ trễ gửi tin của từng nguồn trước khi coi đó là tín hiệu thị trường.

**Phòng tránh.** Cấu hình `stale-days` theo nguồn, có grace period và cảnh báo “nguồn im lặng” riêng; không dùng `is_active` làm tín hiệu kinh doanh khi chưa loại trừ lỗi producer.

---

## 4. Rủi ro luồng, chất lượng và khử trùng

### 4.1. R02/R10 – Offset, checkpoint và replay

`bronze_jobs.py:222-230` dùng `subscribePattern`, `startingOffsets` mặc định `earliest`, `maxOffsetsPerTrigger=10000` và `failOnDataLoss=false` với ghi chú “dev: retention ngắn có thể xóa offset chưa đọc”. Checkpoint nằm trong PVC `/checkpoints/bronze_jobs` (`:252-257`); `stream_province_windows` có checkpoint riêng theo output mode (`stream_province_windows.py:224-232`).

Khi đưa lên prod, retention Kafka ngắn hơn thời gian dừng job sẽ làm mất dữ liệu mà không có lỗi. Checkpoint PVC mất hoặc khôi phục sai làm phát lại trùng/từ bỏ offset. Bronze thiết kế append-only nên trùng là chấp nhận được, nhưng `bronze.ingest_errors` và mart đếm theo sự kiện thì không.

**Xử lý:** đặt retention topic lớn hơn cửa sổ phục hồi tối đa, bật cảnh báo khi `max(offset) - committed offset` vượt ngưỡng, thỉnh thoảng chạy job đọc để kiểm tra offset coverage. Khi khôi phục checkpoint, ghi rõ offset trước/sau vào nhật ký vận hành. Không chuyển `failOnDataLoss` thành `true` khi chưa có quy trình xử lý retention, nếu không job sẽ dừng ngoài dự kiến.

### 4.2. R11 – Watermark và state của cửa sổ tỉnh

`stream_province_windows.py:8-16` mô tả event time `crawl_ts`, watermark mặc định 10 phút, cửa sổ 5 phút, output `update`; `:79-99` định nghĩa aggregation; `:102-126` upsert MongoDB theo `_id = "<đầu cửa sổ>|<mã tỉnh>"` nên chạy lại có cùng kết quả; `:167-180` log số dòng bỏ do watermark. Trên cụm ngày 2026-10-24 có 8/4/7 dòng sau gộp bị bỏ khi tiêm 300 tin trễ (`docs/report/bai-hoc-kinh-nghiem.md:42-53`).

Rủi ro prod: nguồn có độ trễ thực lớn hơn 10 phút thì số tin theo tỉnh thiếu, nhưng silver vẫn đúng vì silver lấy bản crawl mới nhất không dùng watermark cửa sổ. Hai nơi sẽ “khác số” mà không có lỗi đỏ.

**Xử lý:** đo phân vị độ trễ giữa `crawl_ts` và `ingest_ts`; chọn watermark theo phân vị đã đo, log `numRowsDroppedByWatermark` thành metric. Nếu cần số cuối cùng, chạy lại cửa sổ đã đóng bằng nguồn replay; không cố tăng watermark vô hạn vì state sẽ phình to.

### 4.3. R05/R06 – Kết quả thí nghiệm nhiễu không phải bảo chứng cho mọi nguồn

Bảng kết quả robust 97,9–98,1% và lệch trung vị 0,18–0,78% (`docs/thi-nghiem-nhieu.md:289-297`, `deploy/compat.md:13`) được đo trên bộ tiêm nhiễu mô phỏng ba kiểu lỗi gõ được nhận. Đây là cận trên cho lỗi thay ký tự (`docs/thi-nghiem-nhieu.md:274-278`). Khi nguồn thật có kiểu nhiễu khác, không được dùng các con số này để khẳng định độ đúng.

**Phòng tránh:** mỗi nguồn mới phải có tập mẫu thật đối chiếu (`golden set`) và chạy GX riêng trước khi bật topic vào `TOPIC_PATTERN`. Không thêm nguồn mới chỉ bằng cách tạo topic vì regex `jobs\.raw\.[a-z0-9_]+\.v1` sẽ tự nhận (`bronze_jobs.py:28`).

---

## 5. Rủi ro hạ tầng và triển khai

### 5.1. R09 – Xoay khóa S3

`deploy/storage/install.sh:24-30` sinh khóa nếu `.env` trống, `:35-46` tạo Secret SeaweedFS/Postgres, `:79-86` đồng bộ credential mới cho warehouse Lakekeeper. `deploy/lib.sh:18-29` cập nhật Secret `joblens-s3` ở các namespace. Tuy nhiên, pod đã chạy với env từ Secret cũ không tự động nạp lại giá trị mới; Lakekeeper và Spark/Trino có thể ở hai trạng thái khác nhau.

**Triệu chứng:** `InvalidAccessKeyId`, `403 Forbidden` từ SeaweedFS, dbt/Spark báo lỗi ghi metadata dù đọc dashboard cũ vẫn được.

**Xử lý:** chạy lại `bash deploy/storage/install.sh`, sau đó rolling restart các pod dùng `joblens-s3` (Spark streaming, Trino, Airflow worker/task), rồi smoke ghi một dòng. Không dán khóa mới vào terminal/log công khai.

**Phòng tránh:** trong prod dùng danh tính ngắn hạn/IRSA thay vì access key cố định; nếu vẫn dùng key, có runbook xoay theo thứ tự: SeaweedFS → Lakekeeper → Secret từng namespace → restart → smoke. Ghi version credential vào `deploy/compat.md`.

### 5.2. R08 – Trino OOM và pod rollout

`deploy/trino/values.yaml:13-31` đặt `query.maxMemory=1GB`, heap 1400M, limit 2304Mi với ghi chú về lần OOM khi limit 1792Mi. `values-lite.yaml:2-20` dùng 768M/1280Mi và đã được thêm comment cảnh báo không đủ cho dbt. `deploy/lib.sh:92-107` có helper `trino_coord` đợi pod Ready vì pod có thể `Running` nhưng container cũ đã biến mất. `scripts/dbt.sh:18-26` tự reconnect port-forward; `:51-58` retry khi gặp lỗi kết nối.

**Triệu chứng:** `Connection refused`, `container not found ("trino-coordinator")`, pod `OOMKilled` exit 137, `lastState` ghi rõ nguyên nhân trước khi restart.

**Xử lý:** `kubectl describe pod -n serving ...` để đọc `Last State` trước khi khởi động lại; dùng profile full cho dbt; chạy lại `deploy/trino/install.sh`, sau đó `scripts/dbt.sh build`. Lite chỉ dùng cho truy vấn nhẹ; nếu bắt buộc dùng lite thì limits phải ≥2304Mi như comment đã ghi.

**Phòng tránh:** preflight RAM trước bootstrap (`scripts/bootstrap-all.sh:28-30` đã cảnh báo), resource group Trino tách ETL/dashboard (P5), alert OOM/restart, không để dbt và dashboard truy vấn nặng cùng lúc.

### 5.3. R15/R16 – Artifact và Helm

`deploy/airflow/install.sh` hiện có fallback tải chart Airflow 1.22.0 từ GitHub release vì `archive.apache.org` từng timeout (`deploy/compat.md:16`). `deploy/lib.sh:32-59` tag image theo git sha nhưng thêm `-dirty` nếu `spark-jobs` hoặc `dbt/seeds` chưa commit. Các bản chart pinned ở `deploy/storage/install.sh:11-12`, `deploy/trino/values.yaml:7-8`.

**Rủi ro:** mirror GitHub cũng có thể chặn; checksum không được kiểm; Helm release kẹt `pending-install` khi script bị ngắt giữa chừng; người sửa tay qua `kubectl edit` làm manifest lệch thực tế.

**Xử lý:** đưa chart và image vào registry/mirror nội bộ kèm checksum; nếu release kẹt, dùng runbook `helm history`/`helm rollback`, chỉ can thiệp vào secret release khi đã sao lưu và có người phê duyệt. Sau mọi sửa tay, cập nhật lại manifest theo quy tắc repo.

**Phòng tránh:** CI kiểm tra `git diff --quiet` trước khi build image (R18); chạy pre-commit + pytest + `dbt build` trên fixtures trước khi phát hành; mỗi lần demo/chốt số thêm dòng `deploy/compat.md`.

### 5.4. R14/R19 – Môi trường dev không giống prod

Bài học `datetime.UTC` chỉ có từ Python 3.11 nhưng image dùng Python 3.10 nằm ở `docs/report/bai-hoc-kinh-nghiem.md:57-62`. `deploy/kafka/smoke-test.sh` và `deploy/smoke-e2e.sh` phải tự tìm fixtures của repo `joblens` qua `joblens_dir()` (`deploy/lib.sh:72-90`) vì máy sạch không có clone cạnh repo.

**Rủi ro:** prod không có uv cache, không có `$HOME/joblens`, hoặc phiên bản fixtures khác commit contract đã dùng. Smoke đỏ hoặc, tệ hơn, smoke xanh nhưng dữ liệu test không đại diện.

**Phòng tránh:** đóng gói fixtures theo tag trong artifact riêng (không commit dữ liệu crawl), truyền đường dẫn rõ ràng qua `JOBLENS_DIR`, chạy smoke trong chính image prod. Giữ phiên bản Python runtime giữa local và image; khóa dependency bằng `uv.lock`/requirements đã giải theo Python 3.10 cho image.

### 5.5. R17 – Giám sát, sao lưu và khôi phục chưa đạt mức sản phẩm

`docs/ke-hoach-mo-rong.md:18-20` nêu rõ khoảng trống: chưa có Prometheus/luật cảnh báo, chưa có SLO, chưa có GitHub Actions. `:41-44` liệt kê P1–P4; `:48-53` nêu mục tiêu production. `docs/report/bai-hoc-kinh-nghiem.md:235-258` ghi rủi ro cụm k3d đổi IP, Kafka một broker, task Airflow bị giết khi tắt scheduler.

**Rủi ro:** dữ liệu cũ hơn N giờ, lag Kafka, GX hỏng, S3 403, Trino restart, Airflow task retry liên tục đều chỉ được phát hiện khi có người mở Grafana. Không có bản sao SeaweedFS/Postgres được thử khôi phục, nên không biết thời gian phục hồi thực tế.

**Phòng tránh tối thiểu trước khi gọi là “production-like”:**

- Prometheus + Alertmanager với bốn luật: dữ liệu mới nhất cũ hơn ngưỡng, consumer lag vượt ngưỡng, GX/DAG failure, S3 403/OOMKilled. Gửi Telegram/email tới kênh nhóm; `<cần đo>` ngưỡng cụ thể sau khi có số liệu.
- SLO độ trễ p99,9 từ lúc tin vào Kafka đến lúc có trong Trino/alerts (`docs/ke-hoach-mo-rong.md:44`). Chưa có số đo thì giữ `<cần đo>`, không viết con số giả.
- Sao lưu bucket `s3://joblens/` và Postgres Lakekeeper/Airflow; diễn tập khôi phục một lần, ghi thời gian thật vào `deploy/compat.md`.
- Quy trình dừng êm: pause DAG, đợi pod task kết thúc rồi mới tắt scheduler; tắt k3d đúng cách trước khi shutdown máy.

---

## 6. Kịch bản kiểm thử đề xuất

Chỉ chạy trên cụm thử nghiệm/có snapshot; không chạy crawler thật, không đăng nhập vào trang nguồn.

| # | Kịch bản | Kết quả mong đợi | Kiểm tra ở đâu |
|---|---|---|---|
| 1 | Gửi message có `schema_version = job_posting.v2` | Dòng vào `ingest_errors`, có cảnh báo; bronze không chứa payload không hiểu được | `bronze.ingest_errors`, log `bronze_jobs` |
| 2 | Gửi 100 tin có `crawl_ts` lùi 20 phút | `numRowsDroppedByWatermark` tăng; silver vẫn lấy tin mới nhất theo `job_uid` | log `stream_province_windows`, silver |
| 3 | Xoay khóa S3 trong môi trường thử | Theo đúng runbook R09; smoke ghi Iceberg thành công sau restart | Lakekeeper API, pod logs |
| 4 | Dừng Trino giữa lúc `dbt build` | `scripts/dbt.sh` chờ Ready và retry; không báo sai kết quả pass | log dbt, `kubectl describe pod` |
| 5 | Chạy `dbt build` với profile lite | Phát hiện sớm OOM; script từ chối hoặc cảnh báo trước khi chạy | `scripts/bootstrap-all.sh:29`, `values-lite.yaml:2-4` |
| 6 | Xóa một dòng snapshot ngày | Phát hiện lỗi qua kiểm tra số dòng/ngày; replay phục hồi | `silver.job_postings_snapshot` |
| 7 | Đổi alias tỉnh trong seed | Có test hồi quy, GX vẫn đạt, replay so sánh trước khi publish | `tests/test_normalize.py`, `lab.clean_diffs` |
| 8 | Gửi tin `"1,000 - 2,000 USD"` mà không set `JOBLENS_USD_VND` | Preflight in rõ tỷ giá đang dùng; không âm thầm dùng 26.000 | log Spark, metadata chạy |
| 9 | Ngắt mạng khi tải chart Helm | Dùng mirror/checksum, không để release kẹt không rõ lý do | `helm history`, log install |
| 10 | Chạy smoke trên máy không có repo `joblens` | Báo thiếu fixtures rõ ràng hoặc dùng artifact đóng gói | `joblens_dir()`, `deploy/smoke-e2e.sh` |

---

## 7. Thứ tự xử lý đề xuất

1. **P0 – trước khi chạy liên tục:** R02, R06, R09, R08, R17. Đây là các lỗi gây mất/sai dữ liệu hoặc không phát hiện được sự cố.
2. **P1 – trước khi mở rộng nguồn:** R01, R03, R04, R05, R07, R10, R11, R12, R13, R14, R15, R16, R18, R20.
3. **P2 – sau khi có số đo:** R19, T1–T6 theo `docs/ke-hoach-mo-rong.md:22-35`; mỗi cải tiến phải có benchmark thật và dòng mới trong `deploy/compat.md`.

Khi đưa nội dung này vào báo cáo IT4931, viết lại thành văn theo quy tắc R3 của nhóm; giữ dẫn chứng `file:line`, phân biệt rõ “đã gặp” và “có thể gặp”, và để nguyên các chỗ `<cần đo>`, `<người kiểm tra>` nếu chưa có số liệu/xác nhận.
