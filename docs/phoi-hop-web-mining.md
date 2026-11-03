<!-- LIÊN QUAN: joblens/docs/adr/0001-ranh-gioi-xu-ly-giua-cac-repo.md, joblens/schemas/README.md – đổi ranh giới hay hợp đồng thì sửa ở joblens trước, rồi cập nhật file này -->
# Phối hợp Big Data ↔ Web Mining

Dành cho cả hai nhóm (BD = `Big-Data-Storage-and-Processing-joblens`, WM = `Web-mining-joblens`): mỗi bên giao gì, nhận
gì, đang ở đâu, việc tiếp theo, và cách WM chạy thử trên nền tảng của BD. File này chỉ là **bảng phối hợp phía BD** –
bản gốc của ranh giới và hợp đồng nằm ở repo `joblens`:

- Ranh giới việc giữa các repo: [`joblens/docs/adr/0001-ranh-gioi-xu-ly-giua-cac-repo.md`](https://github.com/BD-WM-SA-D/joblens/blob/main/docs/adr/0001-ranh-gioi-xu-ly-giua-cac-repo.md) (D1–D7).
- Ai ghi/đọc topic, bảng, API: [`joblens/schemas/README.md`](https://github.com/BD-WM-SA-D/joblens/blob/main/schemas/README.md).
- File nào lên repo nào: [`joblens/docs/repo-routing.md`](https://github.com/BD-WM-SA-D/joblens/blob/main/docs/repo-routing.md) mục E.

Trạng thái dưới đây đọc từ git ngày **2026-11-03**: BD `main` (commit `12646f2`), WM `main` (commit cuối `dd9e4aa`,
2026-10-18), `joblens` `main` (`9750b1a`, contract đều `status: draft`, ADR 0001 chưa duyệt). Nhóm nào làm xong một
dòng thì sửa cột trạng thái và báo nhóm chat.

---

## 1. Nguyên tắc chung (tóm tắt ADR 0001 – chi tiết đọc bản gốc)

| Quy tắc | Nghĩa là |
|---|---|
| Mỗi topic/bảng **một repo ghi** | WM ghi `jobs.raw.*`, `reviews.raw.*`, `dlq.parse_errors.v1`, `silver.job_skills`, `silver.job_dedup`, `silver.review_aspects`, `silver.reco_scores`. BD ghi bronze, `silver.job_postings*`, gold, `events.user.v1`, `alerts.job_match.v1`. Không bên nào ghi chỗ của bên kia – cần thì đọc, hoặc mở issue `cross-repo` |
| **D1** – WM parse | Crawler của WM tách trường và gửi message `job_posting.v1` đã parse vào Kafka; HTML thô (đã che PII) để ở `s3://joblens/raw-html/`. BD không viết parser |
| **D2** – BD chuẩn hóa | BD chuẩn hóa trường có sẵn (lương, tỉnh, kinh nghiệm, cấp bậc) bằng quy tắc + GX. WM trích **kỹ năng** (`silver.job_skills`) |
| **D3** – mô hình WM là image | WM giao image Docker theo tag `x.y.z-gitsha` (không `latest`), kèm image **stub**. BD chỉ viết task Airflow gọi image, không chứa code mô hình |
| **D4** – dịch vụ | WM giao image `search-service` / `mining-service`; BD triển khai trên K8s. Cảnh báo việc làm là job streaming của BD |
| **D5** – khóa | `job_uid = sha1("<source>:<source_job_id>")`. Trùng giữa các trang do WM gom (`silver.job_dedup.dup_group_id`); BD chỉ khử bản trùng chính xác (cùng `job_uid`) bằng `MERGE` |
| Mỗi bên chạy được một mình | BD dùng VietJobs + fixtures thay crawler, stub thay mô hình; WM train/đánh giá trên VietJobs + dữ liệu crawl, không cần cụm |
| Báo cáo tách riêng | Không chép nguyên văn đoạn văn giữa hai báo cáo (R3), không dùng chung một hình (R5). Số đo hiệu năng của BD chỉ được trích kèm tag |

## 2. Bảng giao diện và trạng thái

| # | Giao diện | Hợp đồng (`joblens`) | WM giao | BD làm | WM | BD |
|---|---|---|---|---|---|---|
| 1 | Tin tuyển dụng vào Kafka | `schemas/kafka/job_posting.v1.json` (1.3.0), topic `jobs.raw.<nguồn>.v1`, key `source_job_id` | `crawler/producer.py`: gửi message vào topic | topic (`deploy/kafka/topics.yaml`), job `bronze_jobs` → `bronze.jobs_raw` → silver | 🟡 spider TopCV + parser có, ghi `jsonl`; **chưa có producer Kafka** | ✅ sẵn sàng (đã chạy với VietJobs replay + fixtures) |
| 2 | Lỗi parse | `parse_error.v1.json`, topic `dlq.parse_errors.v1` | parser gửi lỗi vào topic | đọc, báo tỉ lệ lỗi theo nguồn (Grafana) | 🟡 ghi `parse_errors.jsonl`, chưa gửi Kafka | ⬜ chưa có job đọc |
| 3 | Review công ty | `review.v1.json`, topic `reviews.raw.<nguồn>.v1` | crawler review | `bronze.reviews_raw` | ⬜ | ⬜ chưa có job bronze review |
| 4 | Trích kỹ năng | `silver/job_skills.yaml` | image `skill-extractor` (+ stub) ghi `silver.job_skills` | task Airflow gọi image; dbt `gold.dim_skill`, `gold.mart_skill_demand_monthly` | 🟡 baseline từ điển trong `ie/`, chưa có image | ⬜ chưa có task, chưa có 2 bảng gold |
| 5 | Dedup giữa các trang | `silver/job_dedup.yaml` | image `job-dedup` (+ stub) | task Airflow; gold đếm theo `dup_group_id` (`mart_salary_role_region` đang đếm theo `job_uid`, có ghi chú chỗ đổi) | ⬜ | ⬜ chờ bảng |
| 6 | ABSA review | `silver/review_aspects.yaml` | image `absa-scorer` | task Airflow; dbt `gold.mart_company_aspect_sentiment`, `gold.dim_company` | ⬜ | ⬜ |
| 7 | Gợi ý việc làm (đánh giá offline) | `silver/reco_scores.yaml`, `gold/mart_job_reco_candidates.yaml`, ADR 0002 | image `recsys-scorer` | dbt `mart_job_reco_candidates`; `event-generator` ghi `events.user.v1` | ⬜ | ⬜ |
| 8 | Tìm kiếm, gợi ý trực tuyến | `api/search.v1.yaml`, `api/mining.v1.yaml` | image `search-service`, `mining-service` | Deployment + Service trên K8s, đặt `JOBLENS_SEARCH_URL` | ⬜ | ⬜ |
| 9 | Tình trạng dữ liệu | `gold/data_status.yaml` | – | dbt `gold.data_status` | – | ⬜ |

Đã có ở BD và WM dùng được ngay: `bronze.jobs_raw`, `silver.job_postings`, `silver.job_postings_snapshot`,
`gold.dim_location`, `gold.mart_salary_role_region` (đọc qua Trino).

## 3. Việc tiếp theo – theo thứ tự ghép

| Bước | WM | BD | Xong khi |
|---|---|---|---|
| 0 | Duyệt ADR 0001 (cả nhóm) – `joblens` gắn tag contract đầu tiên | | ADR "Đã chấp nhận", có tag `v1.x` |
| 1 | `crawler/producer.py` gửi `jsonl` đã crawl (đã kiểm schema bằng `schema_errors`) vào `jobs.raw.topcv.v1`, lỗi vào `dlq.parse_errors.v1` | chạy `bronze_jobs`, kiểm `source = 'topcv'` vào bronze → silver; bổ sung alias tỉnh nếu tỉ lệ có địa điểm < 90% | Grafana thấy nguồn `topcv`; GX 15/15 đạt với dữ liệu TopCV |
| 2 | image **stub** `skill-extractor` (trả từ điển) theo hợp đồng chạy D3 | task Airflow `skills` sau `gx_validate_silver`; dbt `dim_skill`, `mart_skill_demand_monthly` | DAG xanh với stub |
| 3 | image thật `skill-extractor` + dòng trong `models.yaml` | đổi tag trong values Airflow, ghi `deploy/compat.md` | DAG xanh với image thật |
| 4 | image `job-dedup` (+ stub) | task Airflow `dedup`; gold đếm theo `dup_group_id` | số tin gold = số nhóm trùng |
| 5 | `search-service` (nếu làm WM4) | triển khai trên K8s | app gọi được `GET /search` |
| 6 | review + ABSA, RecSys (nếu làm) | bronze review, mart tương ứng, `event-generator` | – |

## 4. WM chạy thử trên nền tảng BD

**Cách 1 – không cần cụm (WM tự làm):** kiểm message bằng `crawler/common.py::schema_errors` trước khi gửi.

**Cách 2 – trên máy đã dựng cụm BD** ([`tap-duot-demo.md`](tap-duot-demo.md) mục 8, hoặc Codespaces):

| Cần | Cách |
|---|---|
| Gửi thử message vào Kafka | Trong cụm: `joblens-kafka-bootstrap.ingest.svc.cluster.local:9092`. Từ máy: `kubectl port-forward -n ingest svc/joblens-kafka-bootstrap 9092:9092` rồi dùng `localhost:9092` (chỉ thử nhỏ). Mẫu gửi bằng pod: `deploy/kafka/smoke-test.sh` |
| Xem message đã vào | Kafka UI http://localhost:8081 (sau `bash scripts/demo-ui.sh`) |
| Xem bronze/silver | SQLPad http://localhost:3001 hoặc `bash scripts/trino.sh "SELECT source, count(*) FROM iceberg.bronze.jobs_raw GROUP BY 1"` |
| Đọc bảng từ code Python của WM | Trino `http://localhost:8080` (port-forward của `demo-ui.sh`), catalog `iceberg`; hoặc Iceberg REST `http://localhost:8181/catalog` + S3 (thêm `kubectl port-forward -n lakehouse svc/seaweedfs-all-in-one 8333:8333`, rồi `http://localhost:8333`) – biến môi trường `JOBLENS_*` trong `.env.example` |
| Chạy image mô hình như Airflow sẽ chạy | `kubectl run` trong namespace `compute` với Secret `joblens-s3` (BD hướng dẫn khi có image đầu tiên) |

Không gửi dữ liệu crawl ra dịch vụ ngoài; không commit dữ liệu crawl vào repo nào (quy tắc 8).

## 5. Checklist khi WM giao một image mô hình (để BD chạy được ngay)

- [ ] Tag `x.y.z-gitsha`, không `latest`; ghi vào `models.yaml` của WM (tag, S3 path model, snapshot train, metric).
- [ ] Hợp đồng chạy D3: `--input-table … --input-snapshot … --output-table … --run-id …`.
- [ ] Đọc cấu hình từ biến `JOBLENS_*` (S3, catalog, Trino) – không hard-code địa chỉ hay khóa.
- [ ] Ghi bằng `MERGE` hoặc overwrite theo partition (chạy lại không sinh bản trùng); mỗi dòng có `extractor_version`/`dedup_version`.
- [ ] Mã thoát ≠ 0 khi lỗi; log ra stdout (Airflow đọc từ pod).
- [ ] Ghi rõ RAM/CPU cần (cụm dev ~12 GB cho toàn bộ hệ thống) và thời gian chạy ước tính cho 50k tin.
- [ ] Image **stub** cùng giao diện, không cần model, chạy trong vài giây.
- [ ] Nhắn nhóm chat; BD thêm task vào DAG và ghi dòng `deploy/compat.md`.

## 6. Chỗ dễ chồng lấn – đã thống nhất ai làm

| Chủ đề | BD | WM |
|---|---|---|
| **Nhiễu và làm sạch dữ liệu** (yêu cầu thêm của giảng viên IT4931) | Tiêm nhiễu vào các **trường có sẵn** của message (định dạng lương, chính tả tỉnh, mã hóa, ngoại lai, bản trùng chính xác, message hỏng) và đo khả năng xử lý của pipeline chuẩn hóa – chạy trong namespace `lab.*` | Không ảnh hưởng. Nếu WM muốn đánh giá độ bền của parser/IE với nhiễu thì là thí nghiệm riêng của WM |
| **Trùng lặp** | Bản trùng chính xác và bản đến sai thứ tự (`MERGE` theo `job_uid`, lấy `crawl_ts` mới nhất) | Trùng gần giống / giữa các trang (MinHash…), đánh giá P/R/F1 |
| **Lương, kinh nghiệm** | Chuẩn hóa từ trường có sẵn (`salary_text`, `experience_text`) | Chỉ thí nghiệm trích từ mô tả tự do; không ghi vào `silver.job_postings` (ADR 0001 D2) |
| **GraphFrames** (kỹ năng Spark nhóm 6 của IT4931) | Nếu giảng viên yêu cầu: đồ thị trên dữ liệu của BD (tin – tỉnh – cấp bậc – nguồn), bài toán hạ tầng/thuật toán đồ thị của Spark | Đồ thị **kỹ năng** (WM6, `gold.mart_skill_graph_edges` chưa có contract) – BD không làm bài này |
| **Sơ đồ kiến trúc** | Sơ đồ zone/luồng dữ liệu của nền tảng (`docs/kien-truc.md`) | Sơ đồ pipeline mô hình của WM – vẽ riêng, không dùng lại hình của BD (R5) |

## 7. Câu hỏi cần chốt trong nhóm

1. Ai duyệt ADR 0001 và khi nào – hiện cả 7 quyết định ghi `<chờ>`.
2. WM làm những bài nào (WM4 tìm kiếm hay WM5 gợi ý? có ABSA không?) – quyết định các dòng 6–8 ở mục 2.
3. Ngày WM giao producer Kafka và image stub `skill-extractor` đầu tiên: `<ngày>`.
4. Có cần ghép xong trước buổi thuyết trình IT4931 (tuần 13–15) không, hay BD demo bằng VietJobs + stub.
5. `joblens` có để public không (ADR 0001 D6) – ảnh hưởng việc giảng viên cài được repo môn.
