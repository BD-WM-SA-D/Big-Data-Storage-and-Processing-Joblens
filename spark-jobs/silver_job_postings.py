"""bronze.jobs_raw → silver.job_postings (MERGE INTO theo job_uid) và silver.job_postings_snapshot.

Chạy batch (Airflow gọi định kỳ, hoặc tay qua deploy/spark/submit.sh silver_job_postings):
1. Đọc các dòng bronze mới (ingest_ts > mốc đã xử lý, lưu trong thuộc tính bảng silver
   'joblens.bronze_watermark'). --full để xử lý lại toàn bộ bronze.
2. Mỗi job_uid giữ bản crawl mới nhất (theo crawl_ts, rồi offset), first/last_seen = min/max crawl_ts.
3. Chuẩn hóa payload bằng quy tắc (normalize.py – ADR 0001 D2).
4. MERGE INTO silver.job_postings: bản mới hơn thì cập nhật toàn bộ, bản cũ hơn chỉ nới first_seen_ts.
5. Đánh dấu hết hạn (is_active = false) khi qua deadline hoặc không thấy lại sau --stale-days ngày.
6. Ghi snapshot ngày hôm nay vào silver.job_postings_snapshot (chạy lại trong ngày thì ghi đè ngày đó).

Cột theo contract joblens/schemas/tables/silver/job_postings.yaml (0.2.0) và job_postings_snapshot.yaml (0.1.0).
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

import normalize

CATALOG = "lake"
BRONZE = f"{CATALOG}.bronze.jobs_raw"
SILVER = f"{CATALOG}.silver.job_postings"
SNAPSHOT = f"{CATALOG}.silver.job_postings_snapshot"
WATERMARK_PROP = "joblens.bronze_watermark"

SILVER_COLUMNS = [
    "job_uid", "source", "source_job_id", "url", "title", "company_name", "company_key", "salary_text",
    "salary_min_vnd", "salary_max_vnd", "salary_is_negotiable", "salary_currency_original", "location_codes",
    "experience_min_years", "experience_max_years", "level_code", "employment_type_code", "category_source",
    "deadline", "posted_date", "skill_tags", "description", "requirements", "benefits", "first_seen_ts",
    "last_seen_ts", "is_active", "content_hash", "raw_html_uri", "updated_ts",
]  # fmt: skip

NORMALIZED_TYPE = T.StructType(
    [
        T.StructField("title", T.StringType()),
        T.StructField("company_name", T.StringType()),
        T.StructField("company_key", T.StringType()),
        T.StructField("salary_text", T.StringType()),
        T.StructField("salary_min_vnd", T.LongType()),
        T.StructField("salary_max_vnd", T.LongType()),
        T.StructField("salary_is_negotiable", T.BooleanType()),
        T.StructField("salary_currency_original", T.StringType()),
        T.StructField("location_codes", T.ArrayType(T.StringType())),
        T.StructField("experience_min_years", T.DoubleType()),
        T.StructField("experience_max_years", T.DoubleType()),
        T.StructField("level_code", T.StringType()),
        T.StructField("employment_type_code", T.StringType()),
        T.StructField("category_source", T.StringType()),
        T.StructField("deadline", T.DateType()),
        T.StructField("posted_date", T.DateType()),
        T.StructField("skill_tags", T.ArrayType(T.StringType())),
        T.StructField("description", T.StringType()),
        T.StructField("requirements", T.StringType()),
        T.StructField("benefits", T.StringType()),
    ]
)


@F.udf(NORMALIZED_TYPE)
def normalize_udf(
    payload_json,
):  # không ghi type hint: Spark hiểu nhầm thành pandas UDF
    try:
        payload = json.loads(payload_json) if payload_json else {}
    except json.JSONDecodeError:
        payload = {}
    return normalize.normalize_payload(payload)


DDL_SILVER = f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
    job_uid STRING NOT NULL, source STRING NOT NULL, source_job_id STRING NOT NULL, url STRING,
    title STRING NOT NULL, company_name STRING, company_key STRING, salary_text STRING,
    salary_min_vnd BIGINT COMMENT 'VND/tháng', salary_max_vnd BIGINT COMMENT 'VND/tháng',
    salary_is_negotiable BOOLEAN NOT NULL, salary_currency_original STRING,
    location_codes ARRAY<STRING> NOT NULL, experience_min_years DOUBLE COMMENT 'năm',
    experience_max_years DOUBLE COMMENT 'năm', level_code STRING, employment_type_code STRING,
    category_source STRING, deadline DATE, posted_date DATE, skill_tags ARRAY<STRING> NOT NULL,
    description STRING, requirements STRING, benefits STRING,
    first_seen_ts TIMESTAMP NOT NULL, last_seen_ts TIMESTAMP NOT NULL, is_active BOOLEAN NOT NULL,
    content_hash STRING NOT NULL, raw_html_uri STRING, updated_ts TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (months(first_seen_ts), bucket(16, job_uid))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'comment' = 'Tin đã chuẩn hóa – contract joblens silver.job_postings 0.2.0'
)
"""

DDL_SNAPSHOT = f"""
CREATE TABLE IF NOT EXISTS {SNAPSHOT} (
    snapshot_date DATE NOT NULL, job_uid STRING NOT NULL, is_active BOOLEAN NOT NULL,
    salary_min_vnd BIGINT, salary_max_vnd BIGINT, content_hash STRING NOT NULL
)
USING iceberg
PARTITIONED BY (days(snapshot_date))
TBLPROPERTIES ('format-version' = '2', 'comment' = 'Trạng thái tin mỗi ngày – contract joblens silver.job_postings_snapshot 0.1.0')
"""

log = logging.getLogger("silver_job_postings")


def build_updates(bronze: DataFrame, now: datetime | None = None) -> DataFrame:
    """Dòng bronze → một dòng silver cho mỗi job_uid (bản crawl mới nhất), đúng thứ tự cột SILVER_COLUMNS."""
    now_col = F.lit(now).cast("timestamp") if now else F.current_timestamp()
    rows = bronze.withColumn(
        "job_uid", F.sha1(F.concat_ws(":", "source", "source_job_id"))
    )  # = joblens_contracts.job_uid
    latest = Window.partitionBy("job_uid").orderBy(
        F.col("crawl_ts").desc(), F.col("kafka_offset").desc()
    )
    span = Window.partitionBy("job_uid")
    picked = (
        rows.withColumn("_rn", F.row_number().over(latest))
        .withColumn("first_seen_ts", F.min("crawl_ts").over(span))
        .withColumn("last_seen_ts", F.max("crawl_ts").over(span))
        .filter("_rn = 1")
        .withColumn("n", normalize_udf("payload_json"))
    )
    return picked.select(
        "job_uid",
        "source",
        "source_job_id",
        # bronze.jobs_raw 0.1.0 chưa lưu url (url nằm ngoài payload trong message) → null cho tới khi
        # contract bronze thêm cột url (đề xuất gửi repo joblens)
        F.col("url").cast("string").alias("url")
        if "url" in bronze.columns
        else F.lit(None).cast("string").alias("url"),
        F.coalesce(F.col("n.title"), F.lit("")).alias("title"),
        "n.company_name",
        "n.company_key",
        "n.salary_text",
        "n.salary_min_vnd",
        "n.salary_max_vnd",
        F.coalesce(F.col("n.salary_is_negotiable"), F.lit(False)).alias(
            "salary_is_negotiable"
        ),
        "n.salary_currency_original",
        F.coalesce(F.col("n.location_codes"), F.array().cast("array<string>")).alias(
            "location_codes"
        ),
        "n.experience_min_years",
        "n.experience_max_years",
        "n.level_code",
        "n.employment_type_code",
        "n.category_source",
        "n.deadline",
        "n.posted_date",
        F.coalesce(F.col("n.skill_tags"), F.array().cast("array<string>")).alias(
            "skill_tags"
        ),
        "n.description",
        "n.requirements",
        "n.benefits",
        "first_seen_ts",
        "last_seen_ts",
        F.lit(True).alias("is_active"),
        "content_hash",
        "raw_html_uri",
        now_col.alias("updated_ts"),
    )


MERGE_SQL = f"""
MERGE INTO {SILVER} t
USING silver_updates s
ON t.job_uid = s.job_uid
WHEN MATCHED AND s.last_seen_ts >= t.last_seen_ts THEN UPDATE SET
    {", ".join(f"{c} = s.{c}" for c in SILVER_COLUMNS if c not in ("job_uid", "first_seen_ts"))},
    first_seen_ts = LEAST(t.first_seen_ts, s.first_seen_ts)
WHEN MATCHED THEN UPDATE SET
    first_seen_ts = LEAST(t.first_seen_ts, s.first_seen_ts), updated_ts = s.updated_ts
WHEN NOT MATCHED THEN INSERT *
"""


def main(argv: list[str] | None = None) -> None:
    import bronze_jobs

    p = argparse.ArgumentParser(
        description="bronze.jobs_raw → silver.job_postings + snapshot"
    )
    p.add_argument(
        "--full", action="store_true", help="Bỏ qua mốc, xử lý lại toàn bộ bronze"
    )
    p.add_argument(
        "--stale-days",
        type=int,
        default=7,
        help="Không thấy lại sau N ngày → is_active = false",
    )
    p.add_argument(
        "--snapshot-date", default=None, help="Ngày snapshot (mặc định hôm nay, UTC)"
    )
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    spark: SparkSession = bronze_jobs.build_session("silver-job-postings")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    spark.sql(DDL_SILVER)
    spark.sql(DDL_SNAPSHOT)

    props = {
        r["key"]: r["value"]
        for r in spark.sql(f"SHOW TBLPROPERTIES {SILVER}").collect()
    }
    mark = None if args.full else props.get(WATERMARK_PROP)
    bronze = spark.table(BRONZE)
    if mark:
        bronze = bronze.filter(F.col("ingest_ts") > F.lit(mark).cast("timestamp"))
    new_mark = bronze.agg(F.max("ingest_ts")).first()[0]
    if new_mark is None:
        log.info("không có dòng bronze mới sau mốc %s", mark)
    else:
        updates = build_updates(bronze).persist()
        n = updates.count()
        updates.createOrReplaceTempView("silver_updates")
        spark.sql(MERGE_SQL)
        stats = updates.agg(
            F.sum(F.when(F.size("location_codes") == 0, 1).otherwise(0)).alias(
                "no_location"
            ),
            F.sum(
                F.when(
                    F.col("salary_min_vnd").isNull()
                    & F.col("salary_max_vnd").isNull()
                    & ~F.col("salary_is_negotiable"),
                    1,
                ).otherwise(0)
            ).alias("salary_unparsed"),
            F.sum(F.when(F.col("level_code").isNull(), 1).otherwise(0)).alias(
                "no_level"
            ),
        ).first()
        log.info("MERGE %s tin (mốc bronze %s → %s); không xác định địa điểm %s, lương không đọc được %s, chưa có cấp bậc %s",
                 n, mark, new_mark, stats["no_location"], stats["salary_unparsed"], stats["no_level"])  # fmt: skip
        spark.sql(
            f"ALTER TABLE {SILVER} SET TBLPROPERTIES ('{WATERMARK_PROP}' = '{new_mark.isoformat()}')"
        )
        updates.unpersist()

    spark.sql(f"""
        UPDATE {SILVER} SET is_active = false, updated_ts = current_timestamp()
        WHERE is_active AND (deadline < current_date()
              OR last_seen_ts < current_timestamp() - INTERVAL {int(args.stale_days)} DAYS)
    """)  # fmt: skip

    snap = (
        date.fromisoformat(args.snapshot_date)
        if args.snapshot_date
        else datetime.now(timezone.utc).date()
    )
    spark.sql(f"DELETE FROM {SNAPSHOT} WHERE snapshot_date = DATE '{snap.isoformat()}'")
    spark.sql(f"""
        INSERT INTO {SNAPSHOT}
        SELECT DATE '{snap.isoformat()}', job_uid, is_active, salary_min_vnd, salary_max_vnd, content_hash FROM {SILVER}
    """)
    counts = spark.sql(
        f"SELECT source, count(*) AS n, sum(CAST(is_active AS INT)) AS active FROM {SILVER} GROUP BY source"
    ).collect()
    log.info("silver.job_postings: %s; snapshot %s", [r.asDict() for r in counts], snap)


if __name__ == "__main__":
    main()
