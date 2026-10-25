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

Cần Docker, k3d, kubectl, Helm, uv (danh sách và phiên bản: [`deploy/README.md`](deploy/README.md)). Máy 16 GB
dùng profile mặc định; máy 8 GB đặt `JOBLENS_PROFILE=lite`; không có máy đủ RAM thì dùng GitHub Codespaces
([`docs/cloud-codespaces.md`](docs/cloud-codespaces.md)).

```bash
uv sync                              # môi trường Python (pyspark, dbt-trino, GX, kiểm thử)
cp .env.example .env
bash scripts/bootstrap-all.sh        # dựng cụm + Kafka, S3, Iceberg, MongoDB, Trino, nạp VietJobs, silver, gold,
                                     # Grafana, Airflow (~30–40 phút lần đầu; chạy lại an toàn)
bash scripts/demo-ui.sh              # mở Grafana :3000, Kafka UI :8081, Airflow :8082, Trino :8080 …
bash scripts/trino.sh -f scripts/demo-queries.sql   # kiểm tra dữ liệu bronze/silver/gold
uv run pytest -q                     # kiểm thử (Spark local)
```

Không cần crawler: dữ liệu vào từ VietJobs (`deploy/data/`) và fixtures của repo `joblens`. Kiến trúc và mô tả dữ
liệu: [`docs/kien-truc.md`](docs/kien-truc.md). Kịch bản demo: [`docs/demo.md`](docs/demo.md); tập dượt từng bước: [`docs/tap-duot-demo.md`](docs/tap-duot-demo.md); trong buổi demo: [`docs/chay-demo.md`](docs/chay-demo.md).

## Cấu trúc thư mục

```
deploy/            Helm values, manifest K8s, cấu hình k3d – nơi DUY NHẤT ghép toàn hệ thống
deploy/compat.md   bảng phiên bản platform × mining × app × contracts của mỗi lần chạy
deploy/monitoring/dashboards/   dashboard Grafana export JSON
spark-jobs/        streaming bronze, nạp VietJobs, silver, streaming có state → MongoDB, phân tích + MLlib,
                   kiểm tra GX (gx_silver.py), bảo trì Iceberg
dbt/               gold marts + dbt tests
airflow/dags/      DAG điều phối joblens_pipeline (fail-on-error)
scripts/           bootstrap, giao diện demo, thao tác dữ liệu demo, Trino, dbt, thí nghiệm batch vs streaming
benchmarks/        bảng số liệu cuối (batch vs streaming, Spark analytics)
tests/             pytest (Spark local, DAG Airflow)
docs/              nguồn báo cáo Big Data: kiến trúc, demo, đối chiếu yêu cầu môn, bài học kinh nghiệm
docs/adr/          ADR (Lambda/Kappa)
gx/, event-generator/   chưa dùng (GX chạy trong spark-jobs/gx_silver.py; event-generator chưa làm)
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
