# Gold tăng dần: dựng lại toàn bộ vs tăng dần

Thiết kế: [`../docs/bien-doi-du-lieu.md`](../docs/bien-doi-du-lieu.md) mục 3. Đo bằng `bash scripts/bench-dbt-incremental.sh`
(thời gian mỗi model theo `dbt/target/run_results.json`). Chỉ ghi số đo thật; chưa đo thì để `<cần đo>`.

## Lần 1 – `<ngày giờ>`

- Mã: `<commit>`; dữ liệu: silver `<số tin>` tin, history `<số phiên bản>` phiên bản (gồm dữ liệu mô phỏng
  `recrawl_sim --jobs 2000 --days 28 --seed 42`).

| Lần chạy | Model | Trạng thái | Thời gian (s) | Dòng ghi |
|---|---|---|---|---|
| full-refresh | mart_hiring_weekly | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| full-refresh | mart_job_changes_weekly | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| tăng dần (không có dữ liệu mới) | mart_hiring_weekly | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| tăng dần (không có dữ liệu mới) | mart_job_changes_weekly | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| tăng dần (sau một lần sửa tin) | mart_hiring_weekly | `<cần đo>` | `<cần đo>` | `<cần đo>` |
| tăng dần (sau một lần sửa tin) | mart_job_changes_weekly | `<cần đo>` | `<cần đo>` | `<cần đo>` |

Nhận xét: `<cần đo>`
