# Kịch bản demo môn Big Data (IT4931) – nền tảng JobLens

Mục tiêu: trong ~12 phút cho thấy dữ liệu được **di chuyển, lưu trữ, xử lý** thế nào trên nền tảng tự dựng
(Kafka → Spark → Iceberg → Trino/dbt trên Kubernetes), bằng dữ liệu thật VietJobs (48.092 tin), cả đường
**batch** lẫn **streaming**.

> Số liệu nói trong buổi demo phải là số đo thật từ lần chạy hôm đó (quy tắc repo: không bịa số đo).
> Ghi lại vào `deploy/compat.md` sau khi chạy.

## 1. Chuẩn bị (làm trước buổi demo ít nhất 1 ngày)

Máy: Windows + WSL2, cụm k3d `joblens` đã cài theo [`deploy/README.md`](../deploy/README.md).

```bash
cd ~/joblens/Big-Data-Storage-and-Processing-Joblens
git pull && uv sync
bash deploy/kafka/install.sh          # thêm topic jobs.raw.vietjobs.v1
bash deploy/data/upload-vietjobs.sh   # tải VietJobs từ Hugging Face → s3://joblens/raw/vietjobs/
DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze   # đường A: 48k tin vào bronze
bash deploy/spark/submit.sh silver_job_postings                         # bronze → silver
bash scripts/dbt.sh build                                               # seed + gold + test
bash scripts/trino.sh -f scripts/demo-queries.sql                       # kiểm tra mọi truy vấn chạy
```

Ghi lại: thời gian chạy mỗi job, số dòng bronze/silver, tỉ lệ có địa điểm/lương (truy vấn 2) – dùng làm số
liệu trên slide. Chụp màn hình từng bước làm phương án dự phòng.

**RAM:** chạy streaming + replay + Trino cùng lúc cần khoảng 10–12 GB cho WSL. Máy yếu thì hạ
`EXECUTOR_MEMORY=768m` và tắt Trino khi chạy job: `kubectl scale -n serving deploy --all --replicas=0`
(bật lại bằng `--replicas=1`).

## 2. Kịch bản trình bày

| Phút | Phần | Làm gì trên màn hình | Ý chính (bài giảng) |
|---|---|---|---|
| 0–2 | Kiến trúc | Sơ đồ ở `joblens/docs/project-plan/03`; `kubectl get pods -A` | 4 tầng Store/Move/Process/Query (L01); mỗi thành phần một namespace trên K8s |
| 2–4 | Batch (đường A) | Kết quả truy vấn 1: 48k dòng `backfill.vietjobs`; giải thích job `vietjobs --sink bronze` | Bronze append-only, Parquet + Iceberg (L03, L04); phân vùng `days(ingest_ts), source` |
| 4–8 | Streaming (đường B) | Mở 3 terminal (mục 3) – replay 3.000 tin vào Kafka, job streaming nạp vào bronze, đếm tăng dần trên Trino | Kafka topic 6 partition, key = source_job_id (L08); Structured Streaming + checkpoint, exactly-once nhờ commit Iceberg (L10, L11) |
| 8–10 | Silver | Chạy lại `silver_job_postings`; truy vấn 2 và 3 | Chuẩn hóa bằng quy tắc (lương, 34 tỉnh sau sáp nhập 2025, kinh nghiệm, cấp bậc); `MERGE INTO` upsert theo job_uid: bronze có bản trùng (batch + stream) nhưng silver một dòng một tin |
| 10–11 | Gold | Truy vấn 4 và 5; `dbt build` có 12 kiểm thử dữ liệu | dbt sở hữu SQL gold, test chặn dữ liệu sai (L14) |
| 11–12 | Iceberg | Truy vấn 6 (snapshot, time travel), 7 (số file nhỏ do streaming) | Lịch sử phiên bản, tái lập số liệu; cần compaction – phần tiếp theo |

## 3. Phần streaming chạy trực tiếp

Terminal 1 – job streaming (chạy liên tục, micro-batch 10 giây để thấy rõ trong buổi demo):

```bash
bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"
```

Terminal 2 – replay 3.000 tin VietJobs vào Kafka, 50 tin/giây (~1 phút):

```bash
bash deploy/spark/submit.sh vietjobs --sink kafka --limit 3000 --rate 50
```

Terminal 3 – xem bronze tăng dần:

```bash
watch -n 5 'bash scripts/trino.sh "SELECT kafka_topic, count(*) FROM iceberg.bronze.jobs_raw GROUP BY 1"'
```

Sau khi replay xong: dừng terminal 1 (`Ctrl+C`, rồi `kubectl delete pod -n compute -l spark-role=driver`
nếu driver còn chạy), chạy `bash deploy/spark/submit.sh silver_job_postings` để thấy silver không tăng số tin
(3.000 tin replay trùng job_uid với bản batch).

## 4. Phương án dự phòng

- Mất mạng: image và dữ liệu đã nằm trong cụm, chỉ cần không xóa cụm. Không chạy `upload-vietjobs.sh` trong buổi demo.
- Job Spark lỗi giữa chừng: dùng ảnh chụp ở bước chuẩn bị; xem log bằng lệnh in ra cuối `submit.sh`.
- Máy treo: bật video quay lại phần streaming (quay khi chuẩn bị).

## 5. Chưa có trong demo (nói là phần tiếp theo)

Great Expectations, Airflow DAG điều phối, compaction/expire snapshot, đo batch vs streaming (ADR Lambda/Kappa),
dedup liên trang và kỹ năng (đợi image của nhóm Web Mining), dữ liệu crawl TopCV vào Kafka.
