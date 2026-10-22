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

### Lần 1 – 2026-10-23 (giờ VN)

- Commit: `b67fd01` (A), `6e86d04` (B) – `spark-jobs/` giống nhau · Profile: `full` · Máy: `<CPU>`, 16 GB RAM, WSL `memory=12GB` (12 luồng) · Spark: cluster mode, 1 executor (1 GB)
- Tham số: N=5000, RATE=100 tin/giây, TRIGGER=10 seconds
- Đường A chạy lúc 00:27–00:35, đường B lúc 00:56–01:04 (lần chạy B dùng lại A bằng `REUSE_A`).

| Đường | Dòng | ID khác nhau | File mới | Trễ p50 (s) | Trễ p95 (s) | Trễ max (s) | Thông lượng (dòng/s) | CPU-giây | GB-giờ | RAM đỉnh (MiB) | Thời gian job (s) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A – batch | 5000 | 5000 | 1 | 122.0 | 122.0 | 122.0 | 41.0 | – | – | – | 468 |
| B – streaming | 5000 | 5000 | 2 | 46.2 | 60.6 | 62.7 | 69.3 | 100 | 0.0714 | 1392 | – |

- Độ đúng: 0 source_job_id chỉ có ở một đường (kỳ vọng 0).
- Streaming khi không tải: RAM đỉnh 1377 MiB, 0.0222 GB-giờ trong 60 giây.
- Nhận xét:
  - Hai đường cho cùng tập khóa (độ đúng 0).
  - B: trễ p95 60,6 giây – đạt chỉ tiêu streaming bronze ≤ 2 phút (`joblens/docs/project-plan/03`, mục Metrics).
  - A: trễ 122 giây là thời gian từ lúc job đọc dữ liệu tới lúc ghi file. Chạy theo lịch mỗi giờ thì độ trễ thực tế
    còn cộng thêm thời gian chờ tới lượt (trung bình ~30 phút, tối đa ~60 phút).
  - B chỉ sinh 2 file trong ~50 giây gửi dữ liệu với trigger 10 giây, nghĩa là mỗi micro-batch có dữ liệu chạy lâu
    hơn 10 giây. Lần sau xem *Batch Duration* trên Spark UI (tab Structured Streaming) để có số đo cụ thể.
  - Thông lượng hai đường không so sánh trực tiếp được: B bị giới hạn bởi RATE 100 tin/giây; A bị chi phối bởi chi
    phí khởi động job với N nhỏ.
  - "Thời gian job" 468 giây của A gồm cả bước nạp image vào k3d (`k3d image import`, vài phút) – không dùng để so sánh.
  - Chi phí (CPU-giây, GB-giờ, RAM) của A chưa có: vòng lấy mẫu bị lỗi ở lần chạy A (đã sửa ở commit 8e8e173).
    Cần một lần đo đầy đủ không dùng `REUSE_A`.
  - B giữ ~1,4 GB RAM thường trực kể cả khi không có dữ liệu – chi phí cố định của đường streaming.

## Gợi ý các lần đo tiếp

| Lần | Thay đổi | Câu hỏi |
|---|---|---|
| 2 | TRIGGER = "1 minute" | Trigger dài hơn → ít file hơn bao nhiêu, trễ tăng bao nhiêu? |
| 3 | RATE = 500 | Streaming có theo kịp không (trễ p95 có tăng dần)? |
| 4 | Sau `iceberg_maintenance` | Số file của B giảm còn bao nhiêu (chi phí compaction của streaming)? |
