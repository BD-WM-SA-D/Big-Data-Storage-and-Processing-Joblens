"""Nạp tin tuyển dụng thô từ Kafka (jobs.raw.*.v1) vào bảng Iceberg bronze.jobs_raw.

Đường B (streaming) trong thí nghiệm batch vs streaming (docs/project-plan/03): Spark Structured Streaming,
trigger 1 phút, checkpoint trên volume của driver. Chạy với --once để xử lý hết dữ liệu đang có rồi dừng
(trigger availableNow) – dùng cho smoke test và đường A.

Cột và kiểu theo contract joblens/schemas/tables/bronze/jobs_raw.yaml; message theo
joblens/schemas/kafka/job_posting.v1.json. Không parse HTML, không chuẩn hóa (việc của silver – ADR 0001 D1, D2).

Cấu hình qua biến môi trường (xem deploy/spark/):
    JOBLENS_KAFKA_BOOTSTRAP, JOBLENS_CATALOG_URI, JOBLENS_CATALOG_WAREHOUSE,
    JOBLENS_S3_ENDPOINT, JOBLENS_S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    JOBLENS_CHECKPOINT_DIR
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "lake"
TABLE = f"{CATALOG}.bronze.jobs_raw"
TOPIC_PATTERN = r"jobs\.raw\.[a-z0-9_]+\.v1"

# Trường đọc từ value JSON. payload giữ nguyên dạng chuỗi JSON (cột payload_json).
MESSAGE_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("source_job_id", T.StringType()),
        T.StructField("crawl_ts", T.StringType()),
        T.StructField("content_hash", T.StringType()),
        T.StructField("raw_html_uri", T.StringType()),
    ]
)

# Thứ tự và kiểu cột của bronze.jobs_raw (khớp contract – tests/test_bronze_jobs.py kiểm tra)
BRONZE_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "ingest_ts",
    "source",
    "source_job_id",
    "schema_version",
    "crawl_ts",
    "content_hash",
    "raw_html_uri",
    "payload_json",
]
REQUIRED = [c for c in BRONZE_COLUMNS if c != "raw_html_uri"]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    kafka_topic     STRING    NOT NULL COMMENT 'Topic nguồn',
    kafka_partition INT       NOT NULL COMMENT 'Partition',
    kafka_offset    BIGINT    NOT NULL COMMENT 'Offset',
    ingest_ts       TIMESTAMP NOT NULL COMMENT 'Thời điểm ghi vào bronze (UTC)',
    source          STRING    NOT NULL COMMENT 'topcv | itviec | vietjobs | synthetic',
    source_job_id   STRING    NOT NULL COMMENT 'ID tin trên nguồn (Kafka key)',
    schema_version  STRING    NOT NULL COMMENT 'Ví dụ job_posting.v1',
    crawl_ts        TIMESTAMP NOT NULL COMMENT 'Event time – dùng cho watermark',
    content_hash    STRING    NOT NULL COMMENT 'SHA-256 payload',
    raw_html_uri    STRING             COMMENT 'Đường dẫn HTML thô trên S3',
    payload_json    STRING    NOT NULL COMMENT 'Payload nguyên trạng dạng JSON'
)
USING iceberg
PARTITIONED BY (days(ingest_ts), source)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = 'Message jobs.raw.*.v1 nạp nguyên trạng từ Kafka – contract joblens bronze.jobs_raw 0.1.0'
)
"""

log = logging.getLogger("bronze_jobs")


def to_bronze(kafka_df: DataFrame, ingest_ts: Column | None = None) -> DataFrame:
    """Chuyển DataFrame nguồn Kafka (key, value, topic, partition, offset…) thành các cột của bronze.jobs_raw.

    Bản ghi thiếu trường bắt buộc hoặc JSON hỏng bị loại (cột `_valid`); hàm trả về cả cột này để
    job đếm và ghi log, rồi bỏ đi trước khi ghi bảng.
    """
    value = F.col("value").cast("string")
    msg = F.from_json(value, MESSAGE_SCHEMA)
    out = kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        (ingest_ts if ingest_ts is not None else F.current_timestamp()).alias(
            "ingest_ts"
        ),
        msg["source"].alias("source"),
        # Kafka key = source_job_id (docs/03); thiếu key thì lấy từ value
        F.coalesce(F.col("key").cast("string"), msg["source_job_id"]).alias(
            "source_job_id"
        ),
        msg["schema_version"].alias("schema_version"),
        F.to_timestamp(msg["crawl_ts"]).alias("crawl_ts"),
        msg["content_hash"].alias("content_hash"),
        msg["raw_html_uri"].alias("raw_html_uri"),
        F.get_json_object(value, "$.payload").alias("payload_json"),
    )
    valid = F.lit(True)
    for c in REQUIRED:
        valid = valid & F.col(c).isNotNull()
    return out.withColumn("_valid", valid)


def build_session(app_name: str = "bronze-jobs") -> SparkSession:
    """SparkSession với catalog Iceberg REST "lake" → Lakekeeper, file trên SeaweedFS qua S3FileIO."""
    env = os.environ
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", env["JOBLENS_CATALOG_URI"])
        .config(
            f"spark.sql.catalog.{CATALOG}.warehouse",
            env.get("JOBLENS_CATALOG_WAREHOUSE", "joblens"),
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", env["JOBLENS_S3_ENDPOINT"])
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(
            f"spark.sql.catalog.{CATALOG}.client.region",
            env.get("JOBLENS_S3_REGION", "us-east-1"),
        )
        .getOrCreate()
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--once",
        action="store_true",
        help="Xử lý hết dữ liệu đang có rồi dừng (availableNow)",
    )
    p.add_argument(
        "--trigger", default="1 minute", help="Chu kỳ micro-batch khi chạy liên tục"
    )
    p.add_argument("--starting-offsets", default="earliest")
    p.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    spark = build_session()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(DDL)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["JOBLENS_KAFKA_BOOTSTRAP"])
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        # Dev: retention ngắn có thể xóa offset chưa đọc – không dừng job, chỉ cảnh báo
        .option("failOnDataLoss", "false")
        .load()
    )
    rows = to_bronze(kafka_df)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        batch = batch.persist()
        bad = batch.filter(~F.col("_valid")).count()
        good = batch.filter(F.col("_valid")).select(*BRONZE_COLUMNS)
        n = good.count()
        if n:
            good.writeTo(TABLE).append()
        log.info(
            "batch %s: ghi %s dòng, loại %s dòng thiếu trường bắt buộc",
            batch_id,
            n,
            bad,
        )
        batch.unpersist()

    writer = rows.writeStream.foreachBatch(write_batch).option(
        "checkpointLocation",
        os.path.join(
            os.environ.get("JOBLENS_CHECKPOINT_DIR", "/checkpoints"), "bronze_jobs"
        ),
    )
    writer = (
        writer.trigger(availableNow=True)
        if args.once
        else writer.trigger(processingTime=args.trigger)
    )
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
