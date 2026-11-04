# Các lượt đo còn lại cho yêu cầu Spark và ADR Kappa

Bốn lượt chạy ngắn trên cụm để đóng các mục còn 🧪 trong [`yeu-cau-mon-hoc.md`](yeu-cau-mon-hoc.md) (mục 3 và mục 7), và
một kiểm tra còn treo của thí nghiệm nhiễu. Mỗi mục: vai trò, lệnh (terminal nào), dấu hiệu đúng, chỗ ghi số liệu. Số
đo chỉ ghi khi đã chạy thật – chưa chạy thì để `<cần đo>`.

Quy ước terminal như [`tap-duot-demo.md`](tap-duot-demo.md): **T1–T4** là tab Ubuntu, mỗi tab mới chạy trước
`cd ~/joblens/Big-Data-Storage-and-Processing-joblens`.

| # | Lượt | Đóng yêu cầu | Trạng thái |
|---|---|---|---|
| A | Tìm tin bị chấm sai trong thí nghiệm nhiễu (`lab.noise_errors`) | `<cần kiểm tra>` ở `benchmarks/noise_robustness.md` | ✅ xong |
| B | Streaming `--output-mode append` | Spark nhóm 5 "nhiều output mode" | ✅ xong |
| C | Số file bronze trước/sau `iceberg_maintenance` | ADR 0001 điều kiện 3; bài học 4.1 | ✅ xong |
| D | Batch vs streaming lần 2 (không dùng lại phần batch, thêm trigger 1 phút) | ADR 0001 | ✅ xong – [`../benchmarks/batch_vs_streaming.md`](../benchmarks/batch_vs_streaming.md) Lần 2 |
| E | Tối ưu nhiều join: AQE tắt/bật, broadcast, viết lại thứ tự join | Spark nhóm 3 "tối ưu nhiều join" | 🧪 mã + 3 kiểm thử xong |

Thứ tự nên chạy: A → B → C (B tạo file nhỏ cho C) → D → E.

---

## A. Tin bị chấm sai trong thí nghiệm nhiễu

**Vai trò:** ~2 tin `duplicate` bị chấm sai ở chế độ robust (99,8%) dù về lý thuyết phải 100%. `evaluate` nay ghi
từng tin bị chấm sai vào `lab.noise_errors` kèm giá trị ra và giá trị mốc để tìm nguyên nhân.

```bash
# T1
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab evaluate
bash scripts/trino.sh "SELECT run_id, source_job_id, present, title, ref_title, salary_min_vnd, ref_salary_min_vnd, salary_max_vnd, ref_salary_max_vnd FROM iceberg.lab.noise_errors WHERE noise_type = 'duplicate' AND mode = 'robust'"
bash scripts/trino.sh "SELECT noise_type, mode, count(*) FROM iceberg.lab.noise_errors GROUP BY 1, 2 ORDER BY 1, 2"
```
✅ `==> Xong`; truy vấn 1 in ra các tin `duplicate` bị chấm sai (cột nào khác cột `ref_…` là nguyên nhân).

Kết quả (2026-11-04): 3 lần chấm sai `duplicate` đều là tin `47462` ở 3 lượt nhiễu – lương ra `200000000 / NULL`, mốc
`NULL / NULL`. Không phải lỗi gộp bản trùng: kết quả `clean` của 3 lượt nhiễu còn từ mã **trước** bản sửa "một đầu vượt
ngưỡng thì bỏ cả khoảng", còn lượt sạch đã chạy bằng mã mới. Chạy lại `clean` cho đủ 4 lượt rồi `evaluate`: `duplicate`
100%, `salary_outlier` 98,0% → 99,0%; số liệu trong `benchmarks/noise_robustness.md` đã cập nhật. Bài học: mỗi lần đổi mã
chuẩn hóa phải chạy lại `clean` cho **mọi** lượt trước khi `evaluate` (so sánh hai lượt từ hai phiên bản mã là vô nghĩa).

## B. Streaming với output mode `append`

**Vai trò:** lượt 24–25/10 chỉ chạy `update` (mỗi micro-batch ghi các cửa sổ vừa thay đổi). `append` chỉ ghi một cửa sổ
**một lần, khi đã đóng** – tức khi watermark vượt qua cuối cửa sổ. Dùng cửa sổ 1 phút, watermark 2 phút để không phải
đợi lâu; checkpoint riêng (`province_windows_append`).

```bash
# T1
bash scripts/demo-mode.sh streaming
# T2 (chạy mãi)
DRIVER_MEMORY=768m EXECUTORS=0 bash deploy/spark/submit.sh stream_province_windows --output-mode append --window "1 minute" --watermark "2 minutes"
# T3 (chạy mãi) – job bronze, cũng để tạo file nhỏ cho mục C
DRIVER_MEMORY=768m EXECUTOR_MEMORY=768m bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"
# T4 – khi T2 và T3 đều báo idle
bash deploy/data/replay-vietjobs.sh 1000 20
# đợi ~4 phút rồi gửi thêm một ít để watermark vượt qua các cửa sổ đầu
bash deploy/data/replay-vietjobs.sh 100 20
POD=$(kubectl get pods -n compute --no-headers | grep province | grep Running | awk '{print $1}')
kubectl logs -n compute "$POD" | grep -E "bắt đầu|tiến độ batch|cửa sổ×tỉnh" | tail -20
```
✅ Dòng `bắt đầu: … output mode append …`. Trong lượt replay đầu, các dòng `tiến độ batch` có `state` tăng nhưng
**không có** dòng `batch N: … cửa sổ×tỉnh` (cửa sổ chưa đóng nên append chưa ghi gì). Sau lượt replay thứ hai mới có
`batch N: X cửa sổ×tỉnh (mới X, cập nhật 0)` – mỗi cửa sổ được ghi đúng một lần, không có "cập nhật".

Kết quả (2026-11-04, cửa sổ 1 phút, watermark 2 phút, replay 1.000 tin lúc ~14:14:36 UTC rồi 100 tin lúc 14:21:37):

| Batch | Watermark | State (cửa sổ×tỉnh đang giữ) | Ghi vào MongoDB |
|---|---|---|---|
| 2–5 (lượt replay 1) | 14:12:36 → 14:13:06 | 28 → 54 | **không** – chưa cửa sổ nào đóng |
| 6–7 (không có tin mới) | 14:13:16 | 54 | không |
| 8 (sau lượt replay 2) | **14:19:37** | 54 → **21** | **54 cửa sổ×tỉnh (mới 54, cập nhật 0)** |

So với `update` (lượt 24–25/10: mỗi micro-batch ghi lại các cửa sổ vừa đổi, nhiều dòng "cập nhật"): `append` giữ kết
quả trong state cho tới khi watermark vượt qua cuối cửa sổ, rồi ghi **đúng một lần** – 54 cửa sổ của lượt 1 được ghi
cùng lúc ở batch 8, không có lần cập nhật nào; state giảm từ 54 xuống 21 (chỉ còn các cửa sổ của lượt 2, chưa đóng).
Đánh đổi: kết quả cuối không đổi nữa, nhưng trễ ít nhất "độ dài cửa sổ + watermark" và chỉ ghi khi có tin mới đẩy
watermark lên. Ghi chú: log tiến độ của lượt này in trùng batch có 0 dòng (Spark báo cả các lần trigger rỗng với
batchId cũ) nên không đối chiếu được tổng số dòng vào với 1.000 tin – đã sửa (`new_progress` giữ một bản mỗi batchId,
bản có nhiều dòng nhất) cho các lượt sau.

## C. Số file bronze trước/sau bảo trì

**Vai trò:** streaming 10 giây/micro-batch sinh nhiều file Parquet nhỏ. ADR 0001 cần số đo: `iceberg_maintenance` gộp
lại được bao nhiêu.

```bash
# T4 – sau mục B: dừng streaming trước để không commit đồng thời với bảo trì
bash scripts/demo-mode.sh airflow
bash scripts/trino.sh "SELECT count(*) AS so_file, round(avg(file_size_in_bytes) / 1024) AS kb_tb, sum(record_count) AS so_dong FROM iceberg.bronze.\"jobs_raw\$files\""
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh maintenance_iceberg --tables bronze.jobs_raw
bash scripts/trino.sh "SELECT count(*) AS so_file, round(avg(file_size_in_bytes) / 1024) AS kb_tb, sum(record_count) AS so_dong FROM iceberg.bronze.\"jobs_raw\$files\""
```
✅ Lần đếm thứ hai `so_file` nhỏ hơn, `kb_tb` lớn hơn, `so_dong` **không đổi**.

| | Số file | KB trung bình | Số dòng |
|---|---|---|---|
| Trước | 13 | 2.609 | 76.199 |
| Sau | 8 | 4.226 | 76.199 |

(2026-11-04, sau lượt streaming B; `maintenance_iceberg --tables bronze.jobs_raw`, ~18 giây.) Số dòng không đổi – gộp file
không làm mất hay nhân đôi dữ liệu. Còn 8 file vì `rewrite_data_files` chỉ gộp file trong **cùng một phân vùng**
(`days(ingest_ts), source`) và cần ≥ 2 file mỗi phân vùng (`min-input-files 2`) – kỳ vọng còn khoảng một file mỗi phân
vùng; kiểm tra: `SELECT count(DISTINCT partition) FROM iceberg.bronze."jobs_raw$files"` (`<cần đo>`). Lượt streaming này
ngắn nên chỉ sinh vài file nhỏ; lần đo batch vs streaming (mục D) sẽ cho số file sinh ra theo trigger 10 s và 1 phút.

## D. Batch vs streaming lần 2

**Vai trò:** lần 1 dùng lại phần batch (`REUSE_A`) và chỉ có trigger 10 giây. Lần 2 chạy đủ cả hai đường, thêm trigger
1 phút để thấy đánh đổi độ trễ ↔ số file. Script tự tạm dừng Airflow.

```bash
# T1 (mỗi lệnh ~15–25 phút)
bash scripts/benchmark-batch-vs-streaming.sh 5000 100 "10 seconds"
bash scripts/benchmark-batch-vs-streaming.sh 5000 100 "1 minute"
```
✅ Cuối mỗi lần in bảng Markdown (độ trễ p50/p95, thông lượng, số file, CPU-giây, độ đúng = 0). Chép vào
[`../benchmarks/batch_vs_streaming.md`](../benchmarks/batch_vs_streaming.md) mục "Lần 2".

## E. Tối ưu nhiều join

**Vai trò:** đóng mục "tối ưu nhiều join" – cùng một truy vấn 4 bảng (silver ⋈ snapshot ⋈ bronze ⋈ danh mục tỉnh sau
explode) dưới 4 cấu hình, đo thời gian (trung vị 3 lần, sau 1 lần làm nóng) và đếm loại join / số shuffle trong kế hoạch
cuối. Mã: `spark-jobs/bench_joins.py`; kiểm thử `tests/test_bench_joins.py`.

| Cấu hình | Nghĩa |
|---|---|
| `smj_only` | AQE tắt, tắt broadcast: mọi join là sort-merge (mốc) |
| `broadcast` | AQE tắt, ngưỡng broadcast 10 MB |
| `aqe` | AQE bật |
| `aqe_reorder` | AQE bật + viết lại truy vấn: gộp trước khi join, join bảng nhỏ trước, gợi ý broadcast |

Phát hiện ngay từ kiểm thử: với AQE tắt, bảng tỉnh tạo bằng `createDataFrame` **không được broadcast** dù chỉ 34 dòng –
bộ lập kế hoạch tĩnh không có thống kê nên ước lượng kích thước rất lớn. AQE đo kích thước thật lúc chạy và đổi sang
broadcast hash join; gợi ý `broadcast()` tường minh cũng được.

```bash
# T1
EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh bench_joins --repeat 3 2>&1 | grep -A8 "Kết quả\|cùng kết quả"
```
✅ Bảng 4 dòng (`config`, `seconds_median`, … `broadcast_hash_join`, `sort_merge_join`, `shuffle_exchange`,
`aqe_final`) và `Mọi cấu hình cho cùng kết quả: True`. Kế hoạch đầy đủ: `s3://joblens/reports/join-bench/`.

Lần chạy đầu (2026-11-04) bị **OOMKilled** cả với `DRIVER_MEMORY=3g`: `submit.sh` đặt giới hạn RAM pod chế độ local
cố định 2 GiB, nhỏ hơn cả heap JVM. Đã sửa: giới hạn = `DRIVER_MEMORY` + 1 GiB (heap + phần ngoài heap + Python).

Kết quả: `<cần đo>` → [`../benchmarks/join_optimization.md`](../benchmarks/join_optimization.md).
