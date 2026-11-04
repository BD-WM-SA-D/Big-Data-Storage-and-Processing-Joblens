# Thí nghiệm nhiễu – độ bền của pipeline chuẩn hóa

Số liệu do `spark-jobs/noise_lab.py evaluate` ghi vào `iceberg.lab.noise_metrics`, `iceberg.lab.gold_impact`,
`iceberg.lab.clean_diffs` (cách làm: [`../docs/thi-nghiem-nhieu.md`](../docs/thi-nghiem-nhieu.md)). Bảng dưới chép
nguyên văn từ log của job / truy vấn Trino – chưa chạy thì để `<cần đo>`, không ước lượng.

- **baseline:** chuẩn hóa như trước phần 2, message hỏng bị bỏ im lặng.
- **robust:** chuẩn hóa hiện tại của pipeline thật (phần 2), message hỏng vào cách ly kèm lý do.
- **Đúng** = kết quả giống kết quả của *cùng chế độ* trên lượt sạch `r0_s42` (khôi phục), hoặc bị loại đúng (ngoại lai,
  mất thông tin → null), hoặc được cách ly (message hỏng), hoặc chỉ còn một bản mới nhất (trùng, sai thứ tự).

## Lần 1 – 2026-11-04, khoảng 09:55–10:05 (giờ VN)

- Mã: sau commit `fix(normalize): bỏ cả khoảng lương khi một đầu vượt ngưỡng…`; profile `<full|lite>`; Spark local
  mode (`EXECUTORS=0`, driver 2 GB). Dữ liệu VietJobs 48.092 tin, seed 42, 4 lượt 0/10/20/40%.
- Đếm bằng truy vấn Trino trên `iceberg.lab.noise_metrics`, `gold_impact`, `clean_diffs`; làm tròn 1 chữ số thập phân.

**Tỉ lệ xử lý đúng theo loại nhiễu** (gộp lượt 10/20/40%; *số tin* = số tin bị nhiễu loại đó trong cả ba lượt):

| Loại nhiễu | Mong đợi | Số tin | Baseline | Robust |
|---|---|---|---|---|
| `broken_json` | quarantine | 1.218 | 0,0% | 100,0% |
| `missing_required` | quarantine | 1.174 | 0,0% | 100,0% |
| `experience_outlier` | flag | 1.234 | 0,0% | 100,0% |
| `html_entities` | recover | 1.215 | 0,0% | 100,0% |
| `unicode_nfd` | recover | 1.140 | 0,0% | 100,0% |
| `whitespace` | recover | 1.147 | 0,0% | 100,0% |
| `encoding_mojibake` | recover | 1.178 | 0,3% | 100,0% |
| `salary_yearly` | recover | 744 | 0,0% | 98,9% |
| `location_typo` | recover | 1.200 | 2,0% | 88,4% |
| `salary_outlier` | flag | 784 | 61,4% | 98,0% |
| `location_abbrev` | recover | 933 | 46,5% | 74,4% |
| `location_no_accent` | recover | 1.204 | 99,8% | 99,8% |
| `duplicate` | dedup | 1.128 | 100,0% | 99,8% |
| `out_of_order` | keep_latest | 1.195 | 100,0% | 100,0% |
| `missing_fields` | unknown | 1.260 | 100,0% | 100,0% |
| `salary_placeholder` | unknown | 1.164 | 100,0% | 100,0% |
| `salary_format` | recover | 721 | 100,0% | 100,0% |
| `salary_swap` | recover | 686 | 100,0% | 100,0% |

**Theo lượt** (mọi tin bị nhiễu của lượt):

| Lượt | Tin nhiễu | Baseline đúng | Robust đúng |
|---|---|---|---|
| `r10_s42` | 4.830 | 41,70% | 98,03% |
| `r20_s42` | 9.614 | 42,42% | 98,09% |
| `r40_s42` | 19.325 | 42,92% | 97,84% |

**Ảnh hưởng tới số liệu theo tỉnh** (11 tỉnh có ≥ 200 tin có lương ở lượt sạch; % lệch so với lượt sạch cùng chế độ):

| Lượt | Chế độ | Lệch trung vị lương – TB | Lệch trung vị lương – lớn nhất | Lệch số tin – TB |
|---|---|---|---|---|
| `r10_s42` | baseline | 1,014% | 4,0% | 3,241% |
| `r10_s42` | robust | 0,175% | 1,923% | 2,069% |
| `r20_s42` | baseline | 1,014% | 4,0% | 6,098% |
| `r20_s42` | robust | 0,686% | 3,846% | 3,99% |
| `r40_s42` | baseline | 1,842% | 4,0% | 12,138% |
| `r40_s42` | robust | 0,778% | 3,704% | 8,184% |

**Trên dữ liệu sạch, robust khác baseline ở:** 7 tin về địa điểm (cả 7 sửa đúng: `ho chi minh city…`, `thu duc city` →
79), 13 tin về lương (chuỗi 200–500 triệu/tháng có thật trong VietJobs, nay coi là ngoại lai – quyết định chờ
`<người kiểm tra>` xác nhận).

**Đọc kết quả**
- Pipeline cũ xử lý đúng ~42% tin bị nhiễu ở cả ba mức; sau phần 2 ~98%. Tỉ lệ gần như không đổi theo mức nhiễu vì mỗi
  tin nhiễu được xử lý độc lập.
- Loại pipeline cũ đã làm tốt (định dạng lương, đảo min/max, placeholder, mất trường, trùng, sai thứ tự) giữ nguyên.
- Còn thiếu: `location_abbrev` 74,4% (viết tắt 2 ký tự `SG`, `ĐN`… cố ý không đoán); `location_typo` 88,4% (khóa < 5 ký tự
  không sửa; và đây là **cận trên** – bộ tiêm chỉ dùng đúng ba kiểu lỗi mà bộ so khớp nhận); `salary_outlier` 98,0% (gõ thừa
  hai số 0 vào lương thấp, ví dụ 2 triệu → 200 triệu, vẫn dưới ngưỡng 300 triệu nên không bị bắt); `salary_yearly` 98,9%;
  `duplicate` robust 99,8% (≈ 2 tin chấm sai – chưa rõ nguyên nhân, `<cần kiểm tra>`).
- Số tin theo tỉnh vẫn lệch ở chế độ robust (2–8%) vì message hỏng bị cách ly – tin đó không vào silver ở cả hai chế độ;
  khác biệt là robust giữ lại được lý do trong `lab.quarantine` thay vì mất im lặng. Phần lệch còn lại của baseline là tin
  mất tỉnh do mojibake, gõ sai.
