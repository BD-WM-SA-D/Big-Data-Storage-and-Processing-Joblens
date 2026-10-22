# Batch vs streaming – bảng số liệu

Thí nghiệm bắt buộc theo `joblens/docs/project-plan/03` (mục "Batch vs streaming"). Số liệu dùng cho
ADR [`docs/adr/0001-lambda-vs-kappa.md`](../docs/adr/0001-lambda-vs-kappa.md) và báo cáo Big Data.

> Chỉ ghi số đo thật do `scripts/benchmark-batch-vs-streaming.sh` in ra – không làm tròn lại, không sửa tay.
> Mỗi lần đo thêm một khối "Lần đo", kèm commit, cấu hình máy và dòng tương ứng trong `deploy/compat.md`.

## Cách đo

```bash
bash scripts/benchmark-batch-vs-streaming.sh 5000 100 "10 seconds"
```

- **Dữ liệu:** N tin đầu tiên của VietJobs, giống nhau ở hai đường.
- **A – batch:** `vietjobs.py --sink bronze` (một job Spark đọc CSV từ S3, ghi một lần vào `bronze.jobs_raw`).
- **B – streaming:** producer gửi N tin vào `jobs.raw.vietjobs.v1` với RATE tin/giây → `bronze_jobs`
  (Structured Streaming, micro-batch TRIGGER) → `bronze.jobs_raw`.
- **Độ trễ đầu-cuối:** `"$file_modified_time"` của file Parquet chứa dòng − `crawl_ts` của dòng. Với A, mọi dòng có
  cùng `crawl_ts` (lúc job bắt đầu) nên p50 ≈ p95 ≈ thời gian chạy job. Ở chế độ chạy thật, đường A còn phải chờ
  tới lượt lịch (ví dụ mỗi giờ) – độ trễ trung bình thực tế ≈ chu kỳ/2 + thời gian job, tối đa ≈ chu kỳ + thời gian job.
- **Chi phí:** cộng dồn `kubectl top` các pod Spark, lấy mẫu 5 giây (metrics-server cập nhật ~15 giây/lần) → chỉ là
  ước lượng thô, dùng để so sánh hai đường với nhau trên cùng máy.
- Airflow tạm dừng trong lúc đo để task bảo trì không gộp file giữa chừng.

## Lần đo

### Lần 1 – <yyyy-mm-dd>

- Commit: `<sha>` · Profile: `<full|lite>` · Máy: `<CPU, RAM, RAM cấp cho WSL>` · Spark: `<cluster mode, 1 executor | local[2]>`
- Tham số: N=`<…>`, RATE=`<…>` tin/giây, TRIGGER=`<…>`

| Đường | Dòng | ID khác nhau | File mới | Trễ p50 (s) | Trễ p95 (s) | Trễ max (s) | Thông lượng (dòng/s) | CPU-giây | GB-giờ | RAM đỉnh (MiB) | Thời gian job (s) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A – batch | <…> | <…> | <…> | <…> | <…> | <…> | <…> | <…> | <…> | <…> | <…> |
| B – streaming | <…> | <…> | <…> | <…> | <…> | <…> | <…> | <…> | <…> | <…> | – |

- Độ đúng: `<…>` source_job_id chỉ có ở một đường (kỳ vọng 0).
- Streaming khi không tải: RAM đỉnh `<…>` MiB.
- Nhận xét: `<…>`

## Gợi ý các lần đo tiếp

| Lần | Thay đổi | Câu hỏi |
|---|---|---|
| 2 | TRIGGER = "1 minute" | Trigger dài hơn → ít file hơn bao nhiêu, trễ tăng bao nhiêu? |
| 3 | RATE = 500 | Streaming có theo kịp không (trễ p95 có tăng dần)? |
| 4 | Sau `iceberg_maintenance` | Số file của B giảm còn bao nhiêu (chi phí compaction của streaming)? |
