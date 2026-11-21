"""Bảng silver mà image của Web Mining ghi vào – Big Data tạo bảng, image chỉ ghi (ADR 0001 D3, issue #2).

Ranh giới: repo này KHÔNG ghi dòng nào vào các bảng dưới đây (quy tắc 2) – chỉ tạo bảng rỗng đúng contract để
image skill-extractor / job-dedup / absa-scorer ghi bằng pyiceberg, và để dbt có nguồn đọc ngay cả khi chưa chạy image.
Cột, kiểu, partition theo contract ở joblens/schemas/tables/silver/ (tests/test_mining_tables.py so khớp).

Gọi từ bronze_reviews.py (mỗi lượt DAG); chạy riêng:  bash deploy/spark/submit.sh mining_tables
"""

from __future__ import annotations

import logging

CATALOG = "lake"

_PROPS = """TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = '{comment}'
)"""

TABLES: dict[str, tuple[str, str, str]] = {
    # tên bảng: (cột, partition, chú thích)
    "silver.job_skills": (
        """job_uid STRING NOT NULL, skill_id STRING NOT NULL, surface_form STRING NOT NULL, field STRING NOT NULL,
    span_start INT NOT NULL, span_end INT NOT NULL, confidence DOUBLE NOT NULL, extractor_version STRING NOT NULL,
    run_id STRING NOT NULL""",
        "bucket(16, job_uid)",
        "Kỹ năng trích từ tin – image skill-extractor (WM) ghi; contract joblens silver.job_skills 0.1.1",
    ),
    "silver.job_dedup": (
        """job_uid STRING NOT NULL, dup_group_id STRING NOT NULL, score DOUBLE, dedup_version STRING NOT NULL,
    run_id STRING NOT NULL""",
        "bucket(16, job_uid)",
        "Nhóm tin trùng giữa các nguồn – image job-dedup (WM) ghi; contract joblens silver.job_dedup 0.1.1",
    ),
    "silver.review_aspects": (
        """review_uid STRING NOT NULL, company_key STRING NOT NULL, review_date DATE NOT NULL,
    sentence_index INT NOT NULL, sentence_text STRING, aspect STRING NOT NULL, polarity STRING NOT NULL,
    confidence DOUBLE NOT NULL, model_version STRING NOT NULL, run_id STRING NOT NULL""",
        "months(review_date)",
        "Khía cạnh và cảm xúc trong review – image absa-scorer (WM) ghi; contract joblens silver.review_aspects 0.2.0",
    ),
}

log = logging.getLogger("mining_tables")


def ddl(name: str) -> str:
    cols, part, comment = TABLES[name]
    return (
        f"CREATE TABLE IF NOT EXISTS {CATALOG}.{name} (\n    {cols}\n)\nUSING iceberg\n"
        f"PARTITIONED BY ({part})\n{_PROPS.format(comment=comment)}"
    )


def ensure(spark) -> list[str]:
    """Tạo các bảng chưa có; trả về tên bảng vừa tạo. Bảng đã có thì không đụng tới (không ALTER, không ghi)."""
    created = []
    for name in TABLES:
        if not spark.catalog.tableExists(f"{CATALOG}.{name}"):
            spark.sql(ddl(name))
            created.append(name)
    return created


def main() -> None:
    import bronze_jobs

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = bronze_jobs.build_session("mining-tables")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    created = ensure(spark)
    log.info("bảng mới: %s; đã có từ trước: %s", created or "–", [t for t in TABLES if t not in created] or "–")


if __name__ == "__main__":
    main()
