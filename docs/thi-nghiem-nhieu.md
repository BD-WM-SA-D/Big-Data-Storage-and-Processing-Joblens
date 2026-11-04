# Thí nghiệm nhiễu: biến đổi dữ liệu, tiêm nhiễu và xử lý nhiễu

Yêu cầu thêm của giảng viên IT4931 sau checkpoint 26/10: *thực hiện biến đổi dữ liệu, thêm nhiễu vào dữ liệu rồi xử lý
nhiễu*. Thí nghiệm có 3 phần; mỗi phần ghi vai trò, trạng thái, cách chạy và kết quả **đo thật** (chưa đo thì để
`<cần đo>`). Tổng quan yêu cầu môn học: [`yeu-cau-mon-hoc.md`](yeu-cau-mon-hoc.md) mục 6.

| Phần | Vai trò | Trạng thái |
|---|---|---|
| 1. Tiêm nhiễu | Tạo dữ liệu "bẩn" **có kiểm soát** kèm **đáp án** – biết chính xác tin nào bị nhiễu gì, giá trị sạch là gì | ✅ đã chạy trên cụm: 4 lượt 0/10/20/40% |
| 2. Xử lý nhiễu | Làm pipeline chuẩn hóa silver bền hơn: sửa được nhiễu sửa được, gắn cờ giá trị vô lý, cách ly message hỏng kèm lý do | ✅ đã chạy trên cụm (4 lượt × 2 chế độ) |
| 3. Đo | So kết quả pipeline với đáp án: phát hiện/khôi phục bao nhiêu theo từng loại nhiễu, nhiễu làm lệch số liệu gold bao nhiêu | ✅ đã chạy trên cụm – [`../benchmarks/noise_robustness.md`](../benchmarks/noise_robustness.md) |

**Ranh giới (ADR 0001 joblens):** chỉ đụng các trường mà repo này chuẩn hóa (D2: lương, địa điểm, kinh nghiệm, tiêu đề)
và lớp message (trùng y hệt, sai thứ tự, hỏng). Trùng gần giống giữa các trang là dedup của Web Mining (D5) – không làm.
Chi tiết phân việc: [`phoi-hop-web-mining.md`](phoi-hop-web-mining.md) mục 6.

**Không làm bẩn dữ liệu thật:** mọi bảng của thí nghiệm nằm trong namespace Iceberg riêng `lab` (đọc qua Trino là
`iceberg.lab.*`). Bảng `bronze`, `silver`, `gold` và Kafka không bị đụng tới. Bảng `lab.*` là bảng nội bộ của repo
này (không repo nào khác đọc) nên không cần contract ở `joblens`.

---

## Phần 1 – Tiêm nhiễu

**Vai trò.** Muốn đo pipeline xử lý nhiễu tốt đến đâu thì phải biết đáp án. Dữ liệu thật đã bẩn sẵn nhưng không ai biết
chính xác chỗ nào bẩn, nên ta lấy VietJobs, **tự tiêm nhiễu theo tỉ lệ và seed cho trước**, và ghi lại đáp án cho từng
tin bị nhiễu. Cùng seed thì cùng kết quả – chạy lại được, so sánh được giữa các lần sửa pipeline.

**Mã:** `spark-jobs/noise.py` (thuần Python, tất định), `spark-jobs/noise_lab.py` (job Spark, bước `inject`),
`tests/test_noise.py` (17 kiểm thử).

**Cách hoạt động**
1. Đọc VietJobs → message `job_posting.v1` sạch (cùng hàm `vietjobs.to_message` với đường nạp thật).
2. Mỗi tin được chọn nhiễu với xác suất `--rate`; tin được chọn nhận **đúng một** loại nhiễu trong số các loại áp
   dụng được cho tin đó. Seed tính theo `seed:source_job_id` nên mỗi tin luôn bị nhiễu như nhau giữa các lần chạy.
3. Message nhiễu được tính lại `content_hash` như crawler thật; message gửi theo thứ tự (`seq`) như khi đến Kafka.
4. Ghi 3 bảng: `lab.noisy_messages` (message gửi đi, kể cả message hỏng), `lab.noise_truth` (đáp án),
   `lab.noise_runs` (mỗi lượt một dòng). Mỗi lượt có `run_id` = `r<tỉ lệ %>_s<seed>`; chạy lại cùng `run_id` thì ghi đè
   đúng phần của lượt đó.

**18 loại nhiễu**

| Loại | Ví dụ (sạch → nhiễu) | Pipeline phải |
|---|---|---|
| `salary_format` | `15.0 - 22.0 triệu` → `15-22tr`, `15.000.000 - 22.000.000 VNĐ`, `7,5 triệu` | ra đúng lương (recover) |
| `salary_yearly` | → `180 - 264 triệu/năm` | quy về lương tháng (recover) |
| `salary_swap` | → `22 - 15 triệu` | recover |
| `salary_outlier` | → `1500 - 2200 triệu` (gõ thừa hai số 0) | không đưa vào thống kê (flag) |
| `salary_placeholder` | → `N/A`, `-`, `Liên hệ`, chuỗi rỗng | để null, không đoán (unknown) |
| `location_typo` | `hà nội` → `hà nộii`, `hà nội` → `hà nọi` | recover mã tỉnh |
| `location_abbrev` | → `SG`, `TP.HCM`, `HCMC`, `ĐN`, `TP.HN` | recover |
| `location_no_accent` | → `HA NOI` | recover (nhóm đối chứng) |
| `encoding_mojibake` | `Hà Nội` → `HÃ  Ná»™i` (UTF-8 đọc thành Latin-1) | recover tiêu đề, tỉnh, lương |
| `unicode_nfd` | dấu tách rời (NFD) | recover |
| `html_entities` | → `<b>…</b>`, `&nbsp;`, `R &amp; D` | recover tiêu đề |
| `whitespace` | tab, xuống dòng, khoảng trắng thừa trong tiêu đề | recover |
| `experience_outlier` | → `99 năm` | flag |
| `missing_fields` | mất lương và địa điểm | unknown |
| `duplicate` | gửi lại y hệt | silver chỉ một dòng (dedup) |
| `out_of_order` | bản cũ (crawl_ts lùi 1 ngày, lương khác) đến **sau** bản mới | giữ bản mới (keep_latest) |
| `broken_json` | JSON bị cắt cụt | cách ly kèm lý do (quarantine) |
| `missing_required` | thiếu `crawl_ts` | quarantine |

**Kết quả kiểm thử (không cần cụm):** `uv run pytest -q tests/test_noise.py` → 17 passed. Kiểm: tất định theo seed;
`rate = 0` không đổi gì; tỉ lệ tin nhiễu sai lệch < 3 điểm % so với `--rate` (0,1/0,2/0,4 trên 2.000 tin); đủ 18 loại;
message nhiễu theo trường vẫn hợp lệ với schema `job_posting.v1`; nhiễu message đúng hành vi (bản trùng, bản cũ đến
sau, JSON hỏng, thiếu trường).

**Xem trước – pipeline chuẩn hóa hiện tại đã chịu được nhiễu nào** (chạy `normalize.normalize_payload` trên tin sạch và
tin nhiễu rồi so sánh; **dữ liệu tổng hợp 3.000 tin trong kiểm thử, không phải VietJobs** – số đo thật ở phần 3):

| Đã xử lý được (≈100%) | Xử lý một phần | Chưa xử lý (0%) |
|---|---|---|
| `salary_format`, `salary_swap`, `salary_placeholder`, `location_no_accent`, `missing_fields` | `location_abbrev` (một số viết tắt đã có trong danh mục alias), `salary_outlier` (chỉ khi vượt 2 tỷ/tháng) | `salary_yearly`, `location_typo`, `encoding_mojibake`, `unicode_nfd`, `html_entities`, `whitespace`, `experience_outlier` |

Cột "chưa xử lý" là việc của phần 2.

**Chạy trên cụm**

Cụm đang chạy, ở chế độ nào cũng được (job chạy local mode, ~2 GB):
```bash
# thử nhỏ trước
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab inject --rate 0.2 --seed 42 --limit 5000
# đủ 48.092 tin, 4 mức nhiễu cho phần 3
for r in 0 0.1 0.2 0.4; do EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab inject --rate $r --seed 42; done
```
✅ Log có `lượt r20_s42: {"messages": …, "noisy": …, "noisy_ratio": 0.2…, "by_type": {…}}`, rồi
`đã ghi … message (… tin nhiễu) vào lake.lab.noisy_messages` và bảng số tin theo loại; kết thúc `==> Xong`.

Kiểm tra (SQLPad hoặc `bash scripts/trino.sh "…"`):
```sql
SELECT run_id, rate, messages, noisy FROM iceberg.lab.noise_runs ORDER BY run_id;
SELECT noise_type, expected, count(*) AS so_tin FROM iceberg.lab.noise_truth WHERE run_id = 'r20_s42' GROUP BY 1, 2 ORDER BY 3 DESC;
SELECT seq, kafka_key, substr(value, 1, 200) AS dau_message FROM iceberg.lab.noisy_messages WHERE run_id = 'r20_s42' ORDER BY seq LIMIT 20;
```

**Số liệu trên VietJobs (chạy 2026-11-04, seed 42, đủ 48.092 tin):**

| Lượt | Tỉ lệ đặt | Message gốc | Tin nhiễu | Tỉ lệ thực |
|---|---|---|---|---|
| `r0_s42` | 0 | 48.092 | 0 | 0% (mốc sạch) |
| `r10_s42` | 0,1 | 48.092 | 4.830 | 10,04% |
| `r20_s42` | 0,2 | 48.092 | 9.614 | 19,99% |
| `r40_s42` | 0,4 | 48.092 | 19.325 | 40,18% |

Phân bố lượt `r20_s42` theo loại (tổng 9.614): `missing_fields` 642, `html_entities` 620, `location_typo` 613,
`broken_json` 613, `unicode_nfd` 599, `encoding_mojibake` 597, `missing_required` 592, `out_of_order` 591,
`whitespace` 587, `experience_outlier` 587, `location_no_accent` 582, `salary_placeholder` 558, `duplicate` 557,
`location_abbrev` 419, `salary_outlier` 380, `salary_yearly` 374, `salary_format` 369, `salary_swap` 334.

Các loại nhiễu lương và `location_abbrev` ít hơn vì chỉ áp dụng được cho tin có khoảng lương / có đúng một tỉnh nằm
trong danh mục viết tắt; 4 loại nhiễu message áp dụng được cho mọi tin nên đều ~600.

## Phần 2 – Xử lý nhiễu

**Vai trò.** Biến đổi dữ liệu để pipeline **chịu được nhiễu**: sửa những gì sửa được về đúng giá trị, không đưa giá trị
vô lý vào thống kê, và không làm mất dấu message hỏng. Phần này sửa **mã của pipeline thật** (không phải mã riêng cho
thí nghiệm) – nên dữ liệu thật cũng sạch hơn; thí nghiệm chỉ giữ lại cách làm cũ (`robust=False`) để đo so sánh.

**Đã thay đổi gì**

| Bước | Ở đâu | Xử lý loại nhiễu |
|---|---|---|
| Làm sạch chuỗi: sửa mojibake (chỉ khi chuỗi mã hóa lại được Latin-1 và giải mã được UTF-8 – tiếng Việt đúng không bị đụng), bỏ thẻ/entity HTML, NFC, gộp khoảng trắng | `normalize.clean_text` – áp cho tiêu đề, chuỗi lương, địa điểm, kinh nghiệm, cấp bậc, hình thức | `encoding_mojibake`, `unicode_nfd`, `html_entities`, `whitespace` |
| Lương năm → tháng (`/năm`, `mỗi năm`, `per year`…); đơn vị `tỷ` | `normalize.parse_salary` | `salary_yearly` |
| Ngưỡng ngoại lai lương tháng: trên **300 triệu** → null (trước là 2 tỷ) | `normalize.SALARY_MAX_VALID`; kỳ vọng GX dùng cùng hằng số | `salary_outlier` |
| Kinh nghiệm trên **40 năm** → null (khớp kỳ vọng GX ≤ 40) | `normalize.parse_experience` | `experience_outlier` |
| So khớp tỉnh chịu lỗi gõ: chỉ nhận **một lỗi bàn phím** – thiếu một ký tự, lặp một ký tự, đảo hai ký tự kề – trên danh mục alias, khóa ≥ 5 ký tự, và chỉ khi mọi tên khớp cùng một mã tỉnh; bỏ hậu tố `City`/`Province`. (Bản đầu dùng khoảng cách chỉnh sửa ≤ 1 bất kỳ – đã thu hẹp sau khi phần 3 phát hiện đoán nhầm trên dữ liệu sạch, xem "Phát hiện" ở phần 3) | `normalize.fuzzy_location`, `typo_match`, `place_key` | `location_typo`, một phần `location_abbrev` |
| Cách ly message hỏng **kèm lý do** (`invalid_json`, `missing:<cột>`) thay cho đếm rồi bỏ | `bronze_jobs.to_bronze` + `errors()`; bảng mới `bronze.ingest_errors` (bảng nội bộ của repo, không cần contract) | `broken_json`, `missing_required` |
| Bản trùng y hệt, bản cũ đến sau | **Đã có từ trước**: silver giữ bản `crawl_ts` mới nhất mỗi `job_uid` (`silver_job_postings.build_updates`) | `duplicate`, `out_of_order` |

Không làm (có chủ đích): viết tắt 2 ký tự (`SG`, `ĐN`, `HP`, `CT`) không được đoán – quá mơ hồ, và không thêm chúng vào
danh mục alias chỉ để thí nghiệm đạt điểm (sẽ là "học thuộc đề"). Trùng gần giống giữa các trang: Web Mining (D5).

**Thí nghiệm dùng lại đúng mã pipeline.** Bước `noise_lab.py clean` cho message của mỗi lượt đi qua `to_bronze` →
`build_updates` của silver ở hai chế độ, ghi `lab.job_postings` (kết quả silver) và `lab.quarantine` (message cách ly):

| Chế độ | Chuẩn hóa | Message hỏng |
|---|---|---|
| `baseline` | như trước phần 2 (`normalize_payload(robust=False)`) | bỏ im lặng (không ghi cách ly) |
| `robust` | mới (mặc định của pipeline thật) | `lab.quarantine` kèm lý do |

**Kiểm thử:** `tests/test_noise.py` thêm 5 kiểm thử chạy Spark cục bộ (message hỏng chỉ vào cách ly ở chế độ robust và
đúng lý do; bản cũ đến sau bị bỏ; chế độ robust khôi phục 100% các loại `salary_format/yearly/swap`, `encoding_mojibake`,
`unicode_nfd`, `html_entities`, `whitespace`, `location_no_accent`, ≥ 70% `location_typo`; ngoại lai bị loại);
`tests/test_normalize.py` thêm 5, `tests/test_bronze_jobs.py` thêm 1. Toàn repo: `uv run pytest -q` → 207 passed, 1 skipped.

**Xem trước trên dữ liệu tổng hợp** (3.000 tin của kiểm thử, **không phải VietJobs**; tỉ lệ tin chuẩn hóa ra đúng như
tin sạch, hoặc bị loại đúng khi là ngoại lai/mất thông tin):

| Loại nhiễu | Trước phần 2 | Sau phần 2 |
|---|---|---|
| `salary_format`, `salary_swap`, `salary_placeholder`, `location_no_accent`, `missing_fields` | 100% | 100% |
| `salary_yearly` | 0% | 100% |
| `salary_outlier` | 66% | 100% |
| `location_typo` | 0% | 87% |
| `location_abbrev` | 27% | 65% |
| `encoding_mojibake`, `unicode_nfd`, `html_entities`, `whitespace`, `experience_outlier` | 0% | 100% |

Số đo trên VietJobs: phần 3.

**Ảnh hưởng tới pipeline thật** (cần biết trước khi chạy):
- Lần chạy `silver_job_postings` tiếp theo dùng chuẩn hóa mới cho **dòng bronze mới**; dòng silver cũ giữ cách chuẩn hóa
  cũ cho tới khi chạy lại toàn bộ: `EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh silver_job_postings --full`.
- Tin thật có lương > 300 triệu/tháng hoặc kinh nghiệm > 40 năm sẽ thành null – trên VietJobs sạch (`r0_s42`): 1 tin mất
  lương; tin mất kinh nghiệm: `<cần đo>` (phần 3).
- Job streaming `bronze_jobs` ghi thêm `bronze.ingest_errors`; job đang chạy phải khởi động lại mới dùng mã mới.

**Chạy trên cụm**
```bash
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab clean            # 4 lượt × 2 chế độ
# hoặc một lượt: … noise_lab clean --runs r20_s42
```
✅ Log mỗi lượt × chế độ một dòng
`lượt r20_s42, chế độ robust  : … tin silver, … có lương, … có tỉnh, … message cách ly`; kết thúc `==> Xong`.

Kiểm tra:
```sql
SELECT run_id, mode, count(*) AS tin, count(salary_min_vnd) AS co_luong,
       count_if(cardinality(location_codes) > 0) AS co_tinh
FROM iceberg.lab.job_postings GROUP BY 1, 2 ORDER BY 1, 2;
SELECT run_id, reason, count(*) AS so_message FROM iceberg.lab.quarantine GROUP BY 1, 2 ORDER BY 1, 2;
```

**Số liệu trên VietJobs (chạy 2026-11-04, `noise_lab clean`, ~2 phút cho 4 lượt × 2 chế độ):**

| Lượt | Chế độ | Tin vào silver | Có lương | Có tỉnh | Message cách ly |
|---|---|---|---|---|---|
| `r0_s42` | baseline | 48.092 | 34.338 | 47.342 | – |
| `r0_s42` | robust | 48.092 | 34.337 | 47.411 | 0 |
| `r10_s42` | baseline | 47.500 | 33.379 | 45.752 | (bỏ im lặng) |
| `r10_s42` | robust | 47.500 | 33.358 | 46.419 | 592 |
| `r20_s42` | baseline | 46.887 | 32.421 | 44.197 | (bỏ im lặng) |
| `r20_s42` | robust | 46.887 | 32.391 | 45.431 | 1.205 |
| `r40_s42` | baseline | 45.700 | 30.466 | 41.056 | (bỏ im lặng) |
| `r40_s42` | robust | 45.700 | 30.399 | 43.444 | 2.392 |

Lý do cách ly: `r10_s42` 288 `invalid_json` + 304 `missing:crawl_ts`; `r20_s42` 613 + 592; `r40_s42` 1.218 + 1.174.

Đọc bảng:
- **Không mất tin nào:** tin vào silver + tin bị cách ly = 48.092 ở mọi lượt (ví dụ `r20_s42`: 46.887 + 1.205). Message
  trùng và bản cũ đến sau không thành tin mới – silver gộp về một dòng mỗi `job_uid`. Số cách ly ở `r20_s42` khớp đúng
  đáp án (613 `broken_json`, 592 `missing_required`).
- **Có tỉnh:** chế độ robust nhận ra thêm 667 / 1.234 / 2.388 tin ở mức nhiễu 10/20/40% – phần lớn nhờ sửa mojibake, gõ sai,
  hậu tố `City`. Đúng hay nhầm tỉnh: phần 3 đối chiếu đáp án.
- **Có lương:** robust *ít* hơn baseline một chút (−21/−30/−67) vì loại ngoại lai thành null – đó là hành vi mong đợi;
  phần 3 đo xem baseline đã đưa bao nhiêu giá trị sai vào thống kê.
- **Ảnh hưởng tới dữ liệu thật (lượt sạch `r0_s42`):** chế độ robust nhận thêm **69** tin có tỉnh (cần kiểm tra không nhầm
  ở phần 3) và **1** tin mất lương (vượt ngưỡng 300 triệu/tháng hoặc bị hiểu thành lương năm).

## Phần 3 – Đo

**Vai trò.** Trả lời bằng số: pipeline xử lý đúng bao nhiêu phần trăm mỗi loại nhiễu, trước và sau phần 2, và nếu
**không** xử lý thì nhiễu làm lệch số liệu người dùng thấy (trung vị lương, số tin theo tỉnh) bao nhiêu. Đây là kết quả
đưa vào báo cáo (bài học nhóm 1 "chất lượng dữ liệu", nhóm 9 "kiểm thử") và lên Grafana.

**Mã:** `noise_lab.py evaluate` – các hàm `judge`, `metrics`, `gold_impact`, `clean_diffs`; `tests/test_noise.py`
thêm 4 kiểm thử chạy Spark.

**Cách chấm.** Mỗi dòng đáp án được chấm ở **cả hai chế độ**. Mốc so sánh là kết quả của *cùng chế độ* trên lượt sạch
`r0_s42` – để chỉ đo ảnh hưởng của nhiễu, không lẫn khác biệt giữa hai cách chuẩn hóa.

| Hành vi mong đợi | Tính là đúng khi |
|---|---|
| `recover` | trường bị nhiễu (lương / mã tỉnh / tiêu đề; mojibake: cả ba) bằng đúng mốc sạch |
| `flag` | lương (hoặc kinh nghiệm) là null – không vào thống kê |
| `unknown` | lương là null (và mã tỉnh rỗng nếu mất cả địa điểm) – không đoán bừa |
| `dedup`, `keep_latest` | tin có trong silver với tiêu đề + lương bằng mốc sạch (bản cũ không đè bản mới) |
| `quarantine` | tin **không** có trong silver **và** có trong bảng cách ly |

**Ba bảng kết quả**
- `lab.noise_metrics` – theo lượt × chế độ × loại nhiễu: `total`, `ok`, `rate`.
- `lab.gold_impact` – theo lượt × chế độ: số tỉnh (≥ 200 tin có lương ở mốc sạch), trung bình và lớn nhất % lệch trung vị
  lương, trung bình % lệch số tin theo tỉnh.
- `lab.clean_diffs` – trên lượt **sạch**, tin nào robust cho kết quả khác baseline, kèm chuỗi gốc. Dữ liệu sạch không
  có đáp án, nên bảng này để **người** xem lại: các thay đổi đó là sửa đúng hay đoán nhầm (ví dụ 69 tin có thêm tỉnh).

**Kiểm thử (Spark cục bộ, dữ liệu tổng hợp):** mỗi dòng đáp án được chấm đúng một lần mỗi chế độ; robust đạt 100% ở
mojibake, HTML, khoảng trắng, lương năm, NFD trong khi baseline < 100%; message hỏng: robust 100%, baseline 0%; trùng và
sai thứ tự đạt ở cả hai; số tin theo tỉnh lệch ít hơn khi có xử lý; dữ liệu sạch tổng hợp không có khác biệt.

**Chạy trên cụm** (sau `inject` và `clean`):
```bash
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab evaluate
```
✅ Log in 3 bảng markdown – `Tỉ lệ xử lý đúng theo loại nhiễu`, `Theo lượt`, `Lệch so với mốc sạch` – và dòng
`Dữ liệu sạch – số tin robust khác baseline: {…}`; kết thúc `==> Xong`. Chép 3 bảng vào
[`../benchmarks/noise_robustness.md`](../benchmarks/noise_robustness.md).

**Grafana** – cập nhật dashboard: `bash deploy/monitoring/install.sh`, rồi tải lại trang. Cuối dashboard có mục
*Thí nghiệm nhiễu* gồm 3 ô: bảng xử lý đúng theo loại nhiễu (lượt 20%, cột baseline/robust vẽ thanh màu 0–100%, sắp
theo mức cải thiện – dễ đọc hơn biểu đồ cột vì nhiều loại có tỉ lệ thấp ở baseline); biểu đồ tỉ lệ đúng gộp theo mức
nhiễu (10/20/40%); bảng lệch số liệu theo tỉnh. Các ô báo lỗi đỏ nếu chưa chạy `evaluate`.

Xem mẫu thay đổi trên dữ liệu sạch:
```sql
SELECT field, count(*) FROM iceberg.lab.clean_diffs GROUP BY 1;
SELECT * FROM iceberg.lab.clean_diffs WHERE field = 'location' LIMIT 30;
```

**Phát hiện từ `lab.clean_diffs` (lần đánh giá đầu, 2026-11-04) – đoán nhầm tỉnh trên dữ liệu SẠCH.** Trên lượt sạch,
robust khác baseline ở 77 tin về địa điểm và 13 tin về lương. Xem 30 tin địa điểm đầu tiên: phần lớn là **so khớp gần
đúng nhầm** tên huyện/thị xã thật sang một tỉnh khác chỉ khác một ký tự:

| Chuỗi gốc | Đúng là | Bị đoán thành | Kiểu sai khác |
|---|---|---|---|
| `văn giang` | huyện của Hưng Yên | 91 An Giang | thừa một ký tự (`v`) |
| `hải châu` | quận của Đà Nẵng | 12 Lai Châu | thay một ký tự |
| `phúc yên`, `phổ yên` | Phú Thọ (Vĩnh Phúc cũ), Thái Nguyên | 66 Đắk Lắk (Phú Yên cũ) | thừa / thay một ký tự |
| `tp phủ lý` | Ninh Bình (Hà Nam cũ) | 79 TP.HCM | thay một ký tự |
| `long thành` | Đồng Nai | 75 Đồng Nai (đúng – nhưng nhờ may: gần `long khánh`) | thay một ký tự |
| `ho chi minh city` | 79 | 79 | đúng – nhờ bỏ hậu tố `City` |

Dữ liệu tổng hợp trong kiểm thử không có tên huyện nên không lộ ra lỗi này – đó là lý do phải có bảng `clean_diffs` và
người xem lại. **Đã sửa:** chỉ nhận ba kiểu lỗi bàn phím (thiếu, lặp, đảo), không nhận thay ký tự hay chèn ký tự lạ;
thêm kiểm thử hồi quy cho 6 chuỗi trên (`tests/test_normalize.py`). Đánh đổi cần ghi rõ trong báo cáo: lỗi gõ kiểu *thay*
một ký tự sẽ không được sửa; và bộ tiêm nhiễu `location_typo` dùng đúng ba kiểu được nhận, nên tỉ lệ khôi phục đo
được của loại này là **cận trên** – dữ liệu thật có cả lỗi thay ký tự.

**Sau khi sửa (đánh giá lại 2026-11-04):** trên dữ liệu sạch, robust còn khác baseline ở **7 tin địa điểm** – cả 7 đều
**sửa đúng** (`ho chi minh city`, `ho chi minh city, district 7/10`, `sala urban area, thu duc city` → 79 TP.HCM; trước
đây không nhận ra tỉnh nào) – và **13 tin lương**: `200.0 - 500.0 triệu` (11 tin), `50.0 - 500.0 triệu` (1), `500.0 triệu`
(1). Đây là chuỗi lương có thật trong VietJobs, vượt ngưỡng ngoại lai 300 triệu/tháng. Không có đáp án cho dữ liệu thật
nên đây là **quyết định**, cần người kiểm tra xác nhận (`<người kiểm tra>`): coi là ngoại lai/khả nghi, không đưa vào thống
kê lương (13 trên 34.338 tin có lương, 0,04%). Lần đánh giá này cũng lộ một lỗi nhỏ: khi chỉ **một đầu** vượt ngưỡng,
bản đầu giữ lại nửa còn lại (`50 - 500 triệu` → chỉ còn 50 triệu) – tạo ra khoảng lương không có trong tin gốc. Đã sửa:
một đầu vượt ngưỡng thì bỏ cả khoảng (kiểm thử `test_salary_range_with_one_outlier_end_is_dropped_whole`).

**Kết quả trên VietJobs (2026-11-04)** – chi tiết [`../benchmarks/noise_robustness.md`](../benchmarks/noise_robustness.md):

| | Baseline (trước phần 2) | Robust (sau phần 2) |
|---|---|---|
| Tin bị nhiễu được xử lý đúng – lượt 10 / 20 / 40% | 41,7% / 42,4% / 42,9% | 98,1% / 98,1% / 97,9% |
| Lệch trung vị lương theo tỉnh so với dữ liệu sạch (TB) – 10 / 20 / 40% | 1,01% / 1,01% / 1,84% | 0,18% / 0,69% / 0,78% |
| Lệch số tin theo tỉnh (TB) – 10 / 20 / 40% | 3,2% / 6,1% / 12,1% | 2,1% / 4,0% / 8,2% |
| Message hỏng | mất im lặng | 100% vào cách ly kèm lý do |

Giới hạn còn lại và lý do: xem mục "Đọc kết quả" trong file benchmark.
