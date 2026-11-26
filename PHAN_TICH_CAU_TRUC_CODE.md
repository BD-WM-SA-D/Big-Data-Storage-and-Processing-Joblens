# Phân tích cấu trúc code dự án JobLens (Big-Data-Storage-and-Processing)

## 1. Tổng quan

Dự án **Big-Data-Storage-and-Processing-joblens** là nền tảng Lakehouse cho đồ án IT4931 (Big Data Storage and Processing). Mục tiêu là xây dựng một hệ thống xử lý dữ liệu tuyển dụng Việt Nam quy mô lớn, bao gồm toàn bộ quy trình từ thu thập, lưu trữ, xử lý đến phục vụ dữ liệu.

Kiến trúc được thiết kế theo mô hình **Lambda/Kappa kết hợp** (ADR 0001), chạy trên **Kubernetes (k3d)** với đầy đủ các thành phần của một hệ thống Big Data hiện đại.

## 2. Cấu trúc thư mục

```text
.
├── airflow/          # DAG điều phối pipeline
├── benchmarks/       # Kết quả đo lường batch vs streaming, Spark analytics
├── dbt/              # Mô hình dữ liệu Gold layer
├── deploy/           # Helm manifests, scripts triển khai K8s
├── docs/             # Tài liệu báo cáo, ADR, demo
├── event-generator/  # (Chưa sử dụng)
├── gx/               # (Chưa sử dụng, GX chạy trong spark-jobs)
├── scripts/          # Scripts tiện ích (bootstrap, demo, benchmark)
├── spark-jobs/       # Spark jobs xử lý dữ liệu
├── tests/            # Unit tests
└── pyproject.toml    # Cấu hình dependencies
```

## 3. Phân tích từng thành phần chính

### 3.1. Spark Jobs (`spark-jobs/`)

Đây là lõi xử lý dữ liệu của hệ thống, chứa tất cả các job Spark.

| File | Vai trò | Phân tích chi tiết |
|---|---|---|
| `normalize.py` | **Thư viện chuẩn hóa** | Module thuần Python (~351 dòng), không phụ thuộc Spark. Xử lý chuẩn hóa lương (VND/USD, triệu/ngày/giờ, "thỏa thuận"), địa điểm (map về 34 tỉnh sau sáp nhập 2025 với aliases), kinh nghiệm, cấp bậc, loại hình công việc, tên công ty. Có hàm `@lru_cache` tối ưu lookup. Được gọi qua UDF trong các job Spark. |
| `bronze_jobs.py` | **Kafka → Bronze** | Đọc streaming từ Kafka topics `jobs.raw.*.v1`, parse JSON message theo schema `job_posting.v1`, validate dữ liệu, ghi vào `iceberg.bronze.jobs_raw`. Sử dụng `readStream` + `foreachBatch`, hỗ trợ `--once` (availableNow) cho batch-like processing. Partition theo `days(ingest_ts), source`. |
| `vietjobs.py` | **Nạp dữ liệu VietJobs** | Xử lý dataset VietJobs (LREC 2026). Hỗ trợ 2 chế độ: `--sink bronze` (batch trực tiếp) và `--sink kafka` (replay với tốc độ tùy chỉnh, có thể inject late data để test watermark). Producer Python với `acks=all`, `enable.idempotence`. |
| `silver_job_postings.py` | **Bronze → Silver** | Sử dụng `MERGE INTO` để upsert theo `job_uid = sha1(source:source_job_id)`. Giữ bản crawl mới nhất theo `crawl_ts`, tính `first_seen_ts`/`last_seen_ts`. Gọi `normalize_udf` để chuẩn hóa. Đánh dấu `is_active=false` khi hết hạn hoặc không thấy lại. Tạo snapshot hàng ngày vào `silver.job_postings_snapshot`. |
| `stream_province_windows.py` | **Streaming có trạng thái → MongoDB** | Thể hiện đầy đủ các khái niệm Spark Streaming: `withWatermark`, window aggregation theo event time, state management, output modes (update/append). Ghi kết quả vào MongoDB với upsert idempotent theo composite key. Tự động tạo schema cho Trino connector. Log chi tiết `numRowsDroppedByWatermark`. |
| `analytics_spark.py` | **Phân tích nâng cao + MLlib** | Job phức tạp nhất, thể hiện nhiều kỹ thuật Spark: Window Functions, Pivot/Unpivot, Broadcast Join, Sort-Merge Join, Partition Pruning, Cache/Persist, Pandas UDF (trimmed mean), MLlib Pipeline + CrossValidator. Ghi kết quả vào MongoDB `analytics_*`, lưu execution plans lên S3. |
| `gx_silver.py` | **Data Quality Gate** | Sử dụng Great Expectations Core 1.23. Kiểm tra 15 kỳ vọng: uniqueness, null checks, value sets, range checks, salary logic, coverage thresholds (>=90% có địa điểm, >=50% có lương). Upload kết quả JSON lên S3. Trả exit code 1 nếu fail để block pipeline. |
| `maintenance_iceberg.py` | **Bảo trì Iceberg** | Thực hiện `rewrite_data_files` (binpack 128MB), `rewrite_manifests`, `expire_snapshots` (giữ 10 snapshots gần nhất). Không xóa orphan files để tránh xung đột với streaming đang chạy. |

### 3.2. Airflow (`airflow/dags/`)

| File | Phân tích |
|---|---|
| `joblens_pipeline.py` | DAG chính với dependency rõ ràng: `silver_job_postings >> gx_validate_silver >> [dbt_build_gold, spark_analytics] >> iceberg_maintenance`. Mỗi task chạy trong KubernetesPodOperator, sử dụng image động từ Airflow Variables. Có cấu hình retry, timeout, labels cho monitoring. |
| `joblens_spark.py` | Module helper dựng lệnh `spark-submit`. Hỗ trợ cả cluster mode (production) và local mode (lite profile cho máy 8GB). Tự động cấu hình K8s auth, secrets, env vars. Thuần Python để dễ test. |

### 3.3. dbt (`dbt/`)

| Thành phần | Phân tích |
|---|---|
| `dbt_project.yml` | Cấu hình project, materialization mặc định cho models. |
| `profiles.yml` | Kết nối tới Trino với catalog `iceberg`. |
| `seeds/locations_2025.csv` | Danh mục 34 tỉnh/thành sau sáp nhập 2025, bao gồm cả aliases. Nguồn duy nhất dùng chung giữa `normalize.py` và dbt. |
| `models/sources.yml` | Định nghĩa source `iceberg.silver.job_postings` với data tests (not_null, unique, accepted_values). |
| `models/gold/dim_location.sql` | Dimension table từ seed, ánh xạ thành array aliases. |
| `models/gold/mart_salary_role_region.sql` | Fact table phức tạp sử dụng `GROUPING SETS` để tạo tất cả tổ hợp phân tích (theo tháng, nhóm nghề, cấp bậc, tỉnh) kèm dòng tổng `ALL`. Tính phân vị (p25/p50/p75), tỉ lệ thỏa thuận, lọc tin có >=5 mẫu lương mới tính phân vị. |
| `models/gold/schema.yml` | Data tests cho gold models: row count, uniqueness, referential integrity, accepted values. |
| `macros/generate_schema_name.sql` | Custom macro để kiểm soát schema naming. |
| `tests/generic/joblens_tests.sql` | Custom generic tests. |

### 3.4. Deploy (`deploy/`)

Cấu trúc triển khai rất bài bản, chia theo thành phần:

| Thư mục/File | Phân tích |
|---|---|
| `k3d/` | Tạo cluster local với cấu hình theo profile (full/lite) |
| `kafka/` | Cài Strimzi operator, tạo Kafka cluster KRaft, định nghĩa 8 topics bằng KafkaTopic CRD. |
| `storage/` | Cài SeaweedFS (S3-compatible), Lakekeeper (Iceberg REST catalog), Postgres. |
| `mongodb/` | Cài MongoDB 8.0 cho serving layer. |
| `trino/` | Cài Trino với catalog `iceberg` và `mongodb`. Config theo profile. |
| `spark/` | RBAC, scripts submit job, cấu hình Spark on K8s. |
| `airflow/` | Helm values cho Airflow 3.3.2, RBAC, Postgres. |
| `monitoring/` | Grafana với dashboard custom, Kafka UI (chỉ đọc). |
| `lib.sh` | Thư viện shell chung: build/import image, tạo secrets, load env. |
| `smoke-e2e.sh` | End-to-end smoke test nhanh. |
| `compat.md` | Lịch sử các lần chạy, versioning nghiêm ngặt (không dùng latest). |

### 3.5. Scripts (`scripts/`)

| Script | Phân tích |
|---|---|
| `bootstrap-all.sh` | Orchestrator chính, chạy toàn bộ quy trình dựng cụm từ đầu (~30-40 phút). Idempotent, chạy lại an toàn. |
| `demo-ui.sh` | Port-forward tất cả services ra localhost. |
| `demo-data.sh` | Demo tương tác: SQL trên Iceberg, CRUD MongoDB, gửi tin qua Kafka và theo dõi pipeline. |
| `demo-mode.sh` | Quản lý RAM: chuyển đổi giữa chế độ streaming và airflow (quan trọng cho máy 8GB). |
| `trino.sh` | Wrapper tiện lợi cho Trino CLI. |
| `dbt.sh` | Wrapper chạy dbt trong môi trường. |
| `benchmark-batch-vs-streaming.sh` | Script đo lường benchmark tự động. |

### 3.6. Tests (`tests/`)

Coverage khá tốt với 9 file test:

| File | Phân tích |
|---|---|
| `test_normalize.py` | Test cực kỳ chi tiết cho tất cả rule chuẩn hóa. Cover nhiều edge cases thực tế (VietJobs, TopCV formats). Có test kiểm tra toàn bộ 63 tỉnh cũ đều map được. |
| `test_bronze_jobs.py` | Test transformation Kafka → bronze, so sánh với contract, kiểm tra validation. |
| `test_silver_job_postings.py` | Test MERGE logic, watermark, normalization. |
| `test_stream_province_windows.py` | Test streaming logic, windowing, watermark. |
| `test_analytics_spark.py` | Test các phép tính analytics, joins, aggregations. |
| `test_gx_silver.py` | Test Great Expectations validation. |
| `test_vietjobs.py` | Test parsing và replay VietJobs. |
| `test_airflow_dag.py` | Test DAG structure và dependencies. |
| `conftest.py` | Fixtures chung. |

### 3.7. Docs (`docs/`)

Tài liệu rất đầy đủ và chuyên nghiệp:

- `kien-truc.md`: Mô tả kiến trúc chi tiết với Mermaid diagrams
- `demo.md`: Kịch bản demo cho môn học
- `tap-duot-demo.md`: Hướng dẫn tập dượt từng bước
- `yeu-cau-mon-hoc.md`: Đối chiếu với yêu cầu IT4931
- `adr/0001-lambda-vs-kappa.md`: Quyết định kiến trúc quan trọng
- `cloud-codespaces.md`: Hướng dẫn chạy trên GitHub Codespaces
- `report/`: Báo cáo môn học

## 4. Đánh giá thiết kế

### Ưu điểm

- **Kiến trúc rõ ràng**: Phân tách rõ ràng các layer (Bronze/Silver/Gold), tuân thủ nguyên tắc lakehouse
- **Production-like**: Sử dụng Iceberg với MERGE, watermark, checkpoint, idempotent processing
- **Chất lượng dữ liệu**: Có Data Quality Gate bằng GX, data tests trong dbt
- **Tính toán nâng cao**: Thể hiện tốt các kỹ thuật Spark nâng cao (stateful streaming, window functions, MLlib)
- **Triển khai bài bản**: Toàn bộ hạ tầng định nghĩa bằng code (IaC), dễ tái tạo
- **Testing tốt**: Unit tests cover các logic quan trọng, đặc biệt normalize
- **Tài liệu xuất sắc**: Rất chi tiết, phù hợp cho báo cáo và demo
- **Xử lý thực tế**: Xử lý được nhiều edge case từ dữ liệu thực (VietJobs format)
- **Tối ưu RAM**: Có profile lite cho máy 8GB, local mode cho Spark

### Nhược điểm / Điểm cần cải thiện

- **Thiếu monitoring nâng cao**: Chỉ có Grafana cơ bản, chưa tích hợp Prometheus cho metrics chi tiết
- **GX chưa dùng folder `gx/`**: Code nằm trong `spark-jobs/gx_silver.py` thay vì tận dụng thư mục `gx/` đã tạo
- **event-generator chưa dùng**: Thư mục trống
- **Chưa có CI/CD**: Chỉ có pre-commit, không thấy GitHub Actions
- **Một số hardcode**: Một vài giá trị cấu hình có thể đưa vào ConfigMaps tốt hơn

## 5. Công nghệ sử dụng

| Thành phần | Công nghệ | Phiên bản |
|---|---|---|
| **Orchestration** | Kubernetes (k3d) | 1.35.5 |
| **Message Queue** | Apache Kafka (Strimzi) | 4.3.1 / Strimzi 1.2.0 |
| **Processing** | Apache Spark | 4.1.3 |
| **Storage** | SeaweedFS (S3) | 4.48.0 |
| **Table Format** | Apache Iceberg | 1.11.0 |
| **Catalog** | Lakekeeper (Iceberg REST) | 0.12.0/0.13.x |
| **Transformation** | dbt-trino | 1.10.x |
| **Data Quality** | Great Expectations | 1.23.x |
| **Orchestration** | Apache Airflow | 3.3.2 |
| **Query Engine** | Trino | 483 |
| **NoSQL** | MongoDB | 8.0 |
| **Monitoring** | Grafana | 13.2.4 |

## 6. Kết luận

Dự án có cấu trúc code **rất tốt và chuyên nghiệp**. Điểm nổi bật là:

1. **Thiết kế kiến trúc đúng đắn** - Kết hợp tốt batch và streaming, tuân thủ lakehouse pattern
2. **Code chất lượng cao** - Module hóa tốt, dễ đọc, có comment tiếng Việt rõ ràng
3. **Thể hiện đầy đủ yêu cầu môn học** - Kafka, Spark (batch+streaming), Iceberg, dbt, orchestration, monitoring
4. **Production-ready mindset** - Error handling, validation, idempotency, checkpointing
5. **Tài liệu xuất sắc** - Từ kiến trúc đến demo đều được mô tả chi tiết

Đây là một dự án Big Data hoàn chỉnh, thể hiện sự hiểu biết sâu về các thành phần trong hệ sinh thái Big Data hiện đại. Cấu trúc code được tổ chức khoa học, dễ bảo trì và mở rộng.

---

*Phân tích được thực hiện dựa trên toàn bộ codebase trong repo.*
