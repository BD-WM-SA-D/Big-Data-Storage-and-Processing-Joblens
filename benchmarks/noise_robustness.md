# Thí nghiệm nhiễu – độ bền của pipeline chuẩn hóa

Số liệu do `spark-jobs/noise_lab.py evaluate` ghi vào `iceberg.lab.noise_metrics`, `iceberg.lab.gold_impact`,
`iceberg.lab.clean_diffs` (cách làm: [`../docs/thi-nghiem-nhieu.md`](../docs/thi-nghiem-nhieu.md)). Bảng dưới chép
nguyên văn từ log của job / truy vấn Trino – chưa chạy thì để `<cần đo>`, không ước lượng.

- **baseline:** chuẩn hóa như trước phần 2, message hỏng bị bỏ im lặng.
- **robust:** chuẩn hóa hiện tại của pipeline thật (phần 2), message hỏng vào cách ly kèm lý do.
- **Đúng** = kết quả giống kết quả của *cùng chế độ* trên lượt sạch `r0_s42` (khôi phục), hoặc bị loại đúng (ngoại lai,
  mất thông tin → null), hoặc được cách ly (message hỏng), hoặc chỉ còn một bản mới nhất (trùng, sai thứ tự).

## Lần 1 – `<ngày giờ>`

- Commit `<commit>`, profile `<full|lite>`, dữ liệu VietJobs 48.092 tin, seed 42, 4 lượt 0/10/20/40%.

**Tỉ lệ xử lý đúng theo loại nhiễu** (gộp lượt 10/20/40%):

`<cần đo – chép bảng "Tỉ lệ xử lý đúng theo loại nhiễu" từ log>`

**Theo lượt** (mọi tin bị nhiễu của lượt):

`<cần đo – chép bảng "Theo lượt" từ log>`

**Ảnh hưởng tới số liệu theo tỉnh** (tỉnh có ≥ 200 tin có lương ở lượt sạch; % lệch so với lượt sạch cùng chế độ):

`<cần đo – chép bảng "Lệch so với mốc sạch" từ log>`

**Trên dữ liệu sạch, robust khác baseline ở:** `<cần đo>` tin (theo trường). Kiểm tra mẫu bằng
`SELECT * FROM iceberg.lab.clean_diffs LIMIT 50` – ghi số tin sửa đúng / nhầm sau khi người kiểm tra xem:
`<người kiểm tra>`, `<cần đo>`.
