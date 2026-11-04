# Thí nghiệm nhiễu: biến đổi dữ liệu, tiêm nhiễu và xử lý nhiễu

Yêu cầu thêm của giảng viên IT4931 sau checkpoint 26/10: *thực hiện biến đổi dữ liệu, thêm nhiễu vào dữ liệu rồi xử lý
nhiễu*. Thí nghiệm có 3 phần; mỗi phần ghi vai trò, trạng thái, cách chạy và kết quả **đo thật** (chưa đo thì để
`<cần đo>`). Tổng quan yêu cầu môn học: [`yeu-cau-mon-hoc.md`](yeu-cau-mon-hoc.md) mục 6.

| Phần | Vai trò | Trạng thái |
|---|---|---|
| 1. Tiêm nhiễu | Tạo dữ liệu "bẩn" **có kiểm soát** kèm **đáp án** – biết chính xác tin nào bị nhiễu gì, giá trị sạch là gì | 🧪 mã + 17 kiểm thử xong; chờ chạy trên cụm |
| 2. Xử lý nhiễu | Làm pipeline chuẩn hóa silver bền hơn: sửa được nhiễu sửa được, gắn cờ giá trị vô lý, cách ly message hỏng kèm lý do | ⬜ |
| 3. Đo | So kết quả pipeline với đáp án: phát hiện/khôi phục bao nhiêu theo từng loại nhiễu, nhiễu làm lệch số liệu gold bao nhiêu | ⬜ |

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

**Số liệu trên VietJobs:** lượt `r20_s42`: `<cần đo>` message, `<cần đo>` tin nhiễu, phân bố theo loại `<cần đo>`.

## Phần 2 – Xử lý nhiễu

⬜ Chưa làm. Dự kiến: làm sạch văn bản (NFC, sửa mojibake, bỏ thẻ/entity HTML, gộp khoảng trắng), phân tích lương
lương năm → tháng, so khớp tỉnh chịu lỗi chính tả (khoảng cách chỉnh sửa trên danh mục alias) + bổ sung viết tắt, phát
hiện ngoại lai (khoảng cứng + median ± k·MAD theo cấp bậc × vùng), bảng cách ly message hỏng kèm mã lý do (thay cho loại
bỏ im lặng ở bronze), kỳ vọng GX về tỉ lệ cách ly và ngoại lai. Cột mới ở silver thật (cờ ngoại lai) cần thêm contract
ở `joblens` trước.

## Phần 3 – Đo

⬜ Chưa làm. Dự kiến: precision/recall theo loại nhiễu, tỉ lệ khôi phục đúng giá trị sạch, sai số trung vị lương theo
tỉnh ở mức nhiễu 0/10/20/40% khi có và không có phần 2 → `benchmarks/noise_robustness.md` và ô Grafana.
