# Gold tăng dần: dựng lại toàn bộ vs tăng dần

Thiết kế: [`../docs/bien-doi-du-lieu.md`](../docs/bien-doi-du-lieu.md) mục 3. Đo bằng `bash scripts/bench-dbt-incremental.sh`
(thời gian mỗi model theo `dbt/target/run_results.json`). Chỉ ghi số đo thật; chưa đo thì để `<cần đo>`.

## Lần 1 – 2026-11-15 19:40 (giờ VN)

- Mã: `42fff10`; dữ liệu: `silver.job_postings_history` 51.223 phiên bản của 50.095 tin (811 tin từng thay đổi, gồm dữ
  liệu mô phỏng `recrawl_sim --jobs 2000 --days 28 --seed 42`). dbt chạy 2 luồng (hai model song song).

| Lần chạy | Model | Trạng thái | Thời gian (s) | Dòng ghi |
|---|---|---|---|---|
| full-refresh | mart_hiring_weekly | success | 5.6 | 451 |
| full-refresh | mart_job_changes_weekly | success | 5.6 | 12 |
| tăng dần (không có dữ liệu mới) | mart_hiring_weekly | success | 2.8 | 0 |
| tăng dần (không có dữ liệu mới) | mart_job_changes_weekly | success | 2.8 | 0 |
| tăng dần (sau 2 tin demo mới + silver đánh dấu tin hết hạn) | mart_hiring_weekly | success | 3.5 | 303 |
| tăng dần (sau 2 tin demo mới + silver đánh dấu tin hết hạn) | mart_job_changes_weekly | success | 2.7 | 0 |
| tăng dần (sau một lần sửa lương, `demo-data.sh update`) | mart_hiring_weekly | success | 2.8 | 62 |
| tăng dần (sau một lần sửa lương, `demo-data.sh update`) | mart_job_changes_weekly | success | 2.8 | 3 |

Nhận xét:
- Khi không có dữ liệu mới, lần chạy tăng dần không ghi dòng nào (0 tuần bị ảnh hưởng) và mất 2,8 s so với 5,6 s của
  dựng lại toàn bộ – phần còn lại là chi phí cố định của dbt/Trino (kết nối, tạo bảng tạm, câu truy vấn tìm tuần bị
  ảnh hưởng), không phải tính toán trên dữ liệu.
- Dữ liệu còn nhỏ (451 và 12 dòng gold) nên khác biệt tuyệt đối nhỏ; lợi ích chính của tăng dần ở đây là **không ghi
  lại** tuần không đổi (không sinh file/snapshot mới). Ở quy mô nhiều tháng dữ liệu, phần quét và ghi bỏ được sẽ lớn hơn
  – chưa đo.
- Thời gian đo theo `execution_time` của `run_results.json`; hai model chạy song song nên cùng thời gian kết thúc.
- Sau một lần sửa lương: chỉ tuần hiện tại được tính lại – 62/451 dòng `mart_hiring_weekly`, 3/12 dòng
  `mart_job_changes_weekly`; tin demo có 2 phiên bản đúng (v1 đóng lúc 13:18:43, v2 `changed_fields =
  [salary_min_vnd, salary_max_vnd]`).
- **Bài học:** lần chạy có thêm tin mới phải tính lại 303/451 dòng, vì `silver_job_postings` đánh dấu hết hạn các tin
  mô phỏng không thấy lại sau 7 ngày (`UPDATE … updated_ts = current_timestamp()`) → tuần của các tin đó bị coi là
  "đổi". Kết quả vẫn đúng nhưng phạm vi tăng dần rộng ra. Cách thu hẹp (chưa làm): cột thời điểm đổi riêng cho các cột
  mart dùng, hoặc bỏ `is_active` khỏi điều kiện tuần bị ảnh hưởng của `mart_hiring_weekly` (mart này không dùng
  `is_active`).
