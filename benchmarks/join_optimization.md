# Tối ưu nhiều join – kết quả

Số liệu do `spark-jobs/bench_joins.py` in ra log và ghi `s3://joblens/reports/join-bench/<thời điểm>.json` (cách làm:
[`../docs/thi-nghiem-spark.md`](../docs/thi-nghiem-spark.md) mục E). Chép nguyên văn; chưa chạy thì để `<cần đo>`.

## Lần 1 – 2026-11-04 22:06 (giờ VN)

- Mã: sau commit `fix(spark): giới hạn RAM pod chế độ local theo DRIVER_MEMORY`; Spark local mode `local[2]`
  (`EXECUTORS=0`, driver 2 GB), `spark.sql.shuffle.partitions=4`, `--repeat 3` (1 lần làm nóng + 3 lần đo).
- Kích thước: silver 48.095, snapshot 384.760, bronze 96.198 dòng. Kết quả JSON:
  `s3://joblens/reports/join-bench/20261104T150656Z.json`.

| Cấu hình | Trung vị (s) | Min (s) | Max (s) | Broadcast hash join | Sort-merge join | Shuffle (Exchange) | Kế hoạch cuối AQE | Dòng kết quả |
|---|---|---|---|---|---|---|---|---|
| `smj_only` | 2.61 | 2.55 | 2.97 | 0 | 3 | 7 | False | 18 |
| `broadcast` | 2.44 | 2.26 | 3.69 | 2 | 1 | 4 | False | 18 |
| `aqe` | 2.01 | 2.0 | 2.15 | 3 | 0 | 3 | True | 18 |
| `aqe_reorder` | 1.78 | 1.51 | 2.2 | 3 | 0 | 5 | True | 18 |

Mọi cấu hình cho cùng kết quả: True.

**Nhận xét**
- Thứ tự nhanh dần đúng như kỳ vọng: sort-merge toàn bộ 2,61 s → broadcast tĩnh 2,44 s → AQE 2,01 s → AQE + viết lại
  truy vấn 1,78 s (**−32%** so với mốc). Kết quả giống hệt nhau ở cả 4 cấu hình.
- **Broadcast tĩnh** (AQE tắt) đổi được 2/3 join sang broadcast hash join: bảng Iceberg có thống kê kích thước file
  nên bộ lập kế hoạch ước lượng được. Join còn lại vẫn sort-merge vì ước lượng kích thước sau các phép join/explode
  trước đó vượt ngưỡng 10 MB. (Trong kiểm thử với bảng tạo từ Python – không có thống kê – bộ lập kế hoạch tĩnh không
  broadcast được join nào.)
- **AQE** đo kích thước thật sau mỗi stage nên đổi cả 3 join sang broadcast và bỏ bớt shuffle (7 → 3). Biên độ giữa
  các lần đo cũng hẹp nhất (2,0–2,15 s).
- **Viết lại truy vấn** (gộp snapshot và bronze theo `job_uid` trước khi join, join bảng tỉnh nhỏ trước, gợi ý
  `broadcast()`) có nhiều Exchange hơn AQE (5 so với 3 – thêm các bước `groupBy` gộp trước) nhưng mỗi shuffle nhỏ hơn
  nhiều (gộp 384.760 dòng snapshot về một dòng mỗi tin) nên nhanh nhất. Bài học: số shuffle ít chưa chắc nhanh hơn –
  lượng dữ liệu đi qua shuffle mới quyết định.
- Giới hạn: dữ liệu nhỏ (< 0,5 triệu dòng mỗi bảng), chạy local 2 nhân, nên chênh lệch tuyệt đối dưới 1 giây và có
  nhiễu (max của `broadcast` 3,69 s). Mức chênh sẽ lớn hơn khi dữ liệu lớn hơn ngưỡng broadcast và chạy nhiều executor
  – chưa đo.
