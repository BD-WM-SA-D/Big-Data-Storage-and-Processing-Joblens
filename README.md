# Big-Data-Storage-and-Processing-joblens (joblens-platform)

Nền tảng lakehouse của dự án JobLens VN: Kafka → Spark (batch + Structured Streaming) → Iceberg → dbt → Great Expectations → Airflow → Trino, chạy trên Kubernetes (k3d).

> **Phạm vi đánh giá – môn Big Data Storage and Processing (IT4931, học kỳ 20261)**
>
> Dự án JobLens dùng chung hạ tầng dữ liệu với môn Web Mining và Phân tích thiết kế hệ thống (đã được giảng viên đồng ý ngày <dd/mm/yyyy>). Repo này chỉ chứa và đề nghị đánh giá các phần sau:
> - Thiết kế topic Kafka, schema message, cấu hình producer/consumer.
> - Bảng Iceberg, zone bronze/silver/gold, partition spec, compaction.
> - dbt models, các suite Great Expectations, Airflow DAG.
> - Thí nghiệm batch vs streaming và ADR Lambda/Kappa.
> - Manifest/Helm Kubernetes, tài nguyên, monitoring.
>
> Các phần sau thuộc môn khác, nằm ở repo riêng và chỉ được nhắc tới để làm rõ bối cảnh:
> - Spider, parser, dedup và các mô hình khai phá (IE/ABSA/IR/RecSys/Graph): môn Web Mining.
> - Mô hình nghiệp vụ, ca sử dụng, UML và cổng JobLens: môn Phân tích thiết kế hệ thống.

## Thành viên phụ trách

| Thành viên | MSSV | Vai trò trong repo này |
|---|---|---|
| <Họ tên> | <MSSV> | <…> |

## Chạy nhanh (không cần các repo khác)

```bash
# 1. Cài hợp đồng dữ liệu (pin version)
pip install git+https://github.com/BD-WM-SA-D/joblens@v<x.y.z>

# 2. Cấu hình
cp .env.example .env

# 3. Chạy với dữ liệu mẫu (fixtures thay cho crawler thật, image mô hình thay bằng stub)
<lệnh chạy>
```

## Cấu trúc thư mục

```
deploy/            Helm values, manifest K8s, cấu hình k3d – nơi DUY NHẤT ghép toàn hệ thống
deploy/compat.md   bảng phiên bản platform × mining × app × contracts của mỗi lần chạy
deploy/monitoring/dashboards/   dashboard Grafana export JSON
spark-jobs/        streaming bronze, batch backfill, silver jobs, compaction
dbt/               gold marts + dbt tests
gx/                Great Expectations suites
airflow/dags/      DAG điều phối (fail-on-error)
event-generator/   luồng sự kiện synthetic (events.user.v1)
benchmarks/        batch vs streaming, Trino p95, consumer lag…
docs/              nguồn báo cáo Big Data
docs/adr/          ADR (Lambda/Kappa, catalog, object storage)
```

## Tài liệu

- Báo cáo môn: [`docs/`](docs/)
- Quyết định kiến trúc: [`docs/adr/`](docs/adr/)
- Thuật ngữ: [`glossary.md` trong repo joblens](https://github.com/BD-WM-SA-D/joblens/blob/main/glossary.md)

## Phụ thuộc (chỉ để tham khảo, không thuộc phần chấm)

| Repo | Vai trò | Version đang dùng |
|---|---|---|
| [`joblens`](https://github.com/BD-WM-SA-D/joblens) (joblens-contracts) | Schema message/bảng, fixtures, glossary | v<x.y.z> |
| [`Web-mining-joblens`](https://github.com/BD-WM-SA-D/Web-mining-joblens) (joblens-mining) | Crawler ghi vào Kafka; image mô hình chạy bằng KubernetesPodOperator | v<x.y.z> |
| [`PTTKHT-joblens`](https://github.com/BD-WM-SA-D/PTTKHT-joblens) (joblens-app) | Cổng JobLens, chỉ đọc gold qua Trino | v<x.y.z> |

## Chốt số liệu cho báo cáo

| Tag | Ngày | Iceberg tag / snapshot | Ghi chú |
|---|---|---|---|
| `report-bd-v1` | <dd/mm/yyyy> | <…> | <…> |

## Dữ liệu và giấy phép

Repo không chứa dữ liệu crawl. Dữ liệu mẫu nằm trong fixtures của repo `joblens`. Nếu dùng VietJobs (Pham Dinh et al., LREC 2026) thì phải trích dẫn theo giấy phép của dataset.
