"""Nạp review công ty thô từ Kafka (reviews.raw.*.v1) vào bronze.reviews_raw; tạo sẵn các bảng silver của Web Mining.

Đầu vào cho image `absa-scorer` của Web Mining (ADR 0001 D3: WM đọc bronze.reviews_raw, ghi silver.review_aspects).
Cột theo contract joblens/schemas/tables/bronze/reviews_raw.yaml; message theo joblens/schemas/kafka/review.v1.json.
Không parse, không chuẩn hóa – payload giữ nguyên trong payload_json.

Chạy như một bước batch trong DAG (mỗi lượt chạy là một pod mới, không có checkpoint lâu dài), nên đọc Kafka theo
lô từ đầu topic tới offset mới nhất rồi ghi bằng MERGE INTO … WHEN NOT MATCHED THEN INSERT theo khóa (kafka_topic,
kafka_partition, kafka_offset): dòng đã có bị bỏ, lượt chạy lặp lại (retry) không sinh dòng trùng.
Đọc lại từ đầu mỗi lượt là chấp nhận được vì review ít (vài trăm – vài nghìn message, retention topic 30 ngày). Không
dùng offset kế tiếp theo từng partition: với startingOffsets dạng JSON, Spark bắt khai báo MỌI partition của mọi topic
khớp pattern, partition chưa có dữ liệu trong bảng sẽ làm job lỗi. Khi review nhiều lên: chuyển sang Structured
Streaming availableNow với checkpoint trên S3 (như bronze_jobs).
Message thiếu trường bắt buộc / JSON hỏng → bronze.ingest_errors (dùng chung với bronze_jobs).

Job cũng tạo (nếu chưa có) ba bảng silver mà image của WM ghi vào – image không tự tạo bảng (issue #2): DDL theo
contract trong mining_tables.py. Chạy job này một lần là dbt đọc được các bảng đó (rỗng) ngay cả khi chưa có image.

Chạy:  bash deploy/spark/submit.sh bronze_reviews
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

import bronze_jobs

CATALOG = bronze_jobs.CATALOG
TABLE = f"{CATALOG}.bronze.reviews_raw"
TOPIC_PATTERN = r"reviews\.raw\.[a-z0-9_]+\.v1"

MESSAGE_SCHEMA = T.StructType(
    [
        T.StructField("source", T.StringType()),
        T.StructField("source_review_id", T.StringType()),
        T.StructField("company_id", T.StringType()),
        T.StructField("crawl_ts", T.StringType()),
        T.StructField("content_hash", T.StringType()),
    ]
)

REVIEW_COLUMNS = [
    "kafka_topic", "kafka_partition", "kafka_offset", "ingest_ts", "source", "source_review_id", "company_id",
    "crawl_ts", "content_hash", "payload_json",
]  # fmt: skip

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    kafka_topic      STRING    NOT NULL COMMENT 'Topic nguồn',
    kafka_partition  INT       NOT NULL COMMENT 'Partition',
    kafka_offset     BIGINT    NOT NULL COMMENT 'Offset',
    ingest_ts        TIMESTAMP NOT NULL COMMENT 'Thời điểm ghi vào bronze (UTC)',
    source           STRING    NOT NULL COMMENT 'itviec | synthetic',
    source_review_id STRING    NOT NULL COMMENT 'ID review trên nguồn (Kafka key)',
    company_id       STRING    NOT NULL COMMENT 'ID công ty trên nguồn (slug)',
    crawl_ts         TIMESTAMP NOT NULL COMMENT 'Thời điểm crawl',
    content_hash     STRING    NOT NULL COMMENT 'SHA-256 payload',
    payload_json     STRING    NOT NULL COMMENT 'Payload nguyên trạng dạng JSON'
)
USING iceberg
PARTITIONED BY (days(ingest_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = 'Message reviews.raw.*.v1 nạp nguyên trạng từ Kafka – contract joblens bronze.reviews_raw 0.1.0'
)
"""

MERGE_SQL = f"""
MERGE INTO {TABLE} t
USING new_reviews s
ON t.kafka_topic = s.kafka_topic AND t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
WHEN NOT MATCHED THEN INSERT *
"""

log = logging.getLogger("bronze_reviews")


def to_bronze(kafka_df: DataFrame, ingest_ts: Column | None = None) -> DataFrame:
    """DataFrame nguồn Kafka → cột của bronze.reviews_raw + `_valid`, `_reason` (như bronze_jobs.to_bronze)."""
    value = F.col("value").cast("string")
    msg = F.from_json(value, MESSAGE_SCHEMA)
    out = kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        (ingest_ts if ingest_ts is not None else F.current_timestamp()).alias("ingest_ts"),
        msg["source"].alias("source"),
        F.coalesce(F.col("key").cast("string"), msg["source_review_id"]).alias("source_review_id"),
        msg["company_id"].alias("company_id"),
        F.to_timestamp(msg["crawl_ts"]).alias("crawl_ts"),
        msg["content_hash"].alias("content_hash"),
        F.get_json_object(value, "$.payload").alias("payload_json"),
        value.alias("_value"),
        F.col("key").cast("string").alias("_key"),
    )
    valid = F.lit(True)
    for c in REVIEW_COLUMNS:
        valid = valid & F.col(c).isNotNull()
    missing = F.concat_ws(",", *[F.when(F.col(c).isNull(), F.lit(c)) for c in REVIEW_COLUMNS])
    reason = (
        F.when(valid, F.lit(None).cast("string"))
        .when(F.try_parse_json("_value").isNull(), F.lit("invalid_json"))
        .otherwise(F.concat(F.lit("missing:"), missing))
    )
    return (
        out.withColumn("_valid", valid)
        .withColumn("_reason", reason)
        .withColumn("_value_prefix", F.substring("_value", 1, 2000))
        .drop("_value")
    )


def main(argv: list[str] | None = None) -> None:
    import mining_tables

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    spark = bronze_jobs.build_session("bronze-reviews")
    for ns in ("bronze", "silver"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    spark.sql(DDL)
    spark.sql(bronze_jobs.ERRORS_DDL)
    created = mining_tables.ensure(spark)
    if created:
        log.info("đã tạo bảng cho image Web Mining: %s", ", ".join(created))

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["JOBLENS_KAFKA_BOOTSTRAP"])
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    rows = to_bronze(kafka_df).persist()
    bad_rows = bronze_jobs.errors(rows)
    bad = bad_rows.count()
    good = rows.filter(F.col("_valid")).select(*REVIEW_COLUMNS)
    n = good.count()
    if n:
        good.createOrReplaceTempView("new_reviews")
        spark.sql(MERGE_SQL)
    if bad:
        bad_rows.writeTo(bronze_jobs.ERRORS_TABLE).append()
    total = spark.table(TABLE).count()
    log.info("đọc %s message review hợp lệ (MERGE, dòng đã có bị bỏ), %s cách ly; bronze.reviews_raw có %s dòng",
             n, bad, total)  # fmt: skip
    rows.unpersist()


if __name__ == "__main__":
    main()
