# Bảng phiên bản các lần chạy

Mỗi buổi demo và mỗi lần chốt số liệu đều phải dựng lại được từ một dòng của bảng này (quy tắc 4 trong tài liệu kế hoạch, repo `joblens`). Không dùng `latest`. Image ghi tag dạng `version-gitsha`.

| Ngày | platform | mining (images) | app | contracts (`joblens`) | Iceberg tag | Ghi chú |
|---|---|---|---|---|---|---|
| <yyyy-mm-dd> | v<x.y.z> | <image:x.y.z-sha> | v<x.y.z> | v<x.y.z> | <…> | <…> |
| 2026-10-17 | 9432fed (walking skeleton) | – (chưa có image mining) | – | joblens@9750b1a | – | Smoke test fixtures → Kafka → Spark → bronze.jobs_raw → Trino đạt; Strimzi 1.2.0, SeaweedFS chart 4.48.0, Lakekeeper chart 0.12.0, Trino 483, spark-jobs:0.1.0-9432fed |
