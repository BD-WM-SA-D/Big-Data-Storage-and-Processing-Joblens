"""Nạp tin tuyển dụng thô từ Kafka (jobs.raw.*.v1) vào bảng Iceberg bronze.jobs_raw.

Đường B (streaming) trong thí nghiệm batch vs streaming (docs/project-plan/03): Spark Structured Streaming,
trigger 1 phút, checkpoint trên volume của driver. Chạy với --once để xử lý hết dữ liệu đang có rồi dừng
(trigger availableNow) – dùng cho smoke test và đường A.

Mỗi micro-batch ghi bằng MERGE theo khóa Kafka `(kafka_topic, kafka_partition, kafka_offset)` để replay không
nhân bản dòng; tin ngoài allow-list bị cách ly vào bronze.ingest_errors kèm lý do. Sau mỗi batch, bảng nội bộ
bronze.kafka_offset_audit so sánh offset cuối lấy bằng AdminClient với offset lớn nhất đã nạp vào bronze.

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
import re
from datetime import datetime, timezone

from confluent_kafka import KafkaException
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "lake"
TABLE = f"{CATALOG}.bronze.jobs_raw"
TOPIC_PATTERN = r"jobs\.raw\.[a-z0-9_]+\.v1"
ALLOWED_SCHEMA_VERSIONS = {"job_posting.v1"}
ALLOWED_SOURCES = ("topcv", "itviec", "vietjobs", "synthetic")
KEY_COLUMNS = ("kafka_topic", "kafka_partition", "kafka_offset")

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
    'write.merge.mode' = 'merge-on-read',
    'comment' = 'Message jobs.raw.*.v1 nạp nguyên trạng từ Kafka – contract joblens bronze.jobs_raw 0.1.0'
)
"""

ERRORS_TABLE = f"{CATALOG}.bronze.ingest_errors"
ERRORS_DDL = f"""
CREATE TABLE IF NOT EXISTS {ERRORS_TABLE} (
    kafka_topic     STRING    NOT NULL,
    kafka_partition INT       NOT NULL,
    kafka_offset    BIGINT    NOT NULL,
    ingest_ts       TIMESTAMP NOT NULL,
    kafka_key       STRING,
    reason          STRING    NOT NULL COMMENT 'invalid_json | unsupported_schema | unsupported_source:<giá trị> | missing:<cột,…>',
    value_prefix    STRING             COMMENT '2000 ký tự đầu của message'
)
USING iceberg
PARTITIONED BY (days(ingest_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'merge-on-read',
    'comment' = 'Message bị loại khi nạp bronze, kèm lý do (bảng nội bộ của repo Big Data – không có contract)'
)
"""
ERROR_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "ingest_ts",
    "kafka_key",
    "reason",
    "value_prefix",
]

AUDIT_TABLE = f"{CATALOG}.bronze.kafka_offset_audit"
AUDIT_DDL = f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    topic             STRING    NOT NULL COMMENT 'Topic Kafka khớp TOPIC_PATTERN',
    `partition`       INT       NOT NULL COMMENT 'Partition của topic',
    kafka_end_offset  BIGINT    NOT NULL COMMENT 'Offset tiếp theo sẽ ghi, lấy bằng AdminClient',
    bronze_max_offset BIGINT             COMMENT 'Offset lớn nhất đã nạp vào bronze',
    gap               BIGINT    NOT NULL COMMENT 'Số offset chưa nạp = end - 1 - offset lớn nhất',
    measured_at       TIMESTAMP NOT NULL COMMENT 'Thời điểm đo theo UTC'
)
USING iceberg
PARTITIONED BY (days(measured_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'merge-on-read',
    'comment' = 'Kiểm toán offset Kafka cho bronze (bảng nội bộ của repo Big Data – không có contract)'
)
"""
AUDIT_COLUMNS = [
    "topic",
    "partition",
    "kafka_end_offset",
    "bronze_max_offset",
    "gap",
    "measured_at",
]
AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("topic", T.StringType(), False),
        T.StructField("partition", T.IntegerType(), False),
        T.StructField("kafka_end_offset", T.LongType(), False),
        T.StructField("bronze_max_offset", T.LongType(), True),
        T.StructField("gap", T.LongType(), False),
        T.StructField("measured_at", T.TimestampType(), False),
    ]
)

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
        value.alias("_value"),
        F.col("key").cast("string").alias("_key"),
    )
    allowed_schema = F.col("schema_version").isin(*ALLOWED_SCHEMA_VERSIONS)
    allowed_source = F.col("source").isin(*ALLOWED_SOURCES)
    valid = F.lit(True)
    for c in REQUIRED:
        valid = valid & F.col(c).isNotNull()
    valid = (
        valid
        & F.coalesce(allowed_schema, F.lit(False))
        & F.coalesce(allowed_source, F.lit(False))
    )
    missing = F.concat_ws(",", *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED])
    reason = (
        F.when(valid, F.lit(None).cast("string"))
        .when(F.try_parse_json("_value").isNull(), F.lit("invalid_json"))
        .when(F.length(missing) > 0, F.concat(F.lit("missing:"), missing))
        .when(~F.coalesce(allowed_schema, F.lit(True)), F.lit("unsupported_schema"))
        .otherwise(F.concat(F.lit("unsupported_source:"), F.col("source")))
    )
    return (
        out.withColumn("_valid", valid)
        .withColumn("_reason", reason)
        .withColumn("_value_prefix", F.substring("_value", 1, 2000))
        .drop("_value")
    )


def errors(rows: DataFrame) -> DataFrame:
    """Dòng bị loại → bronze.ingest_errors, kèm invalid_json, missing, unsupported_schema hoặc unsupported_source."""
    return rows.filter(~F.col("_valid")).select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "ingest_ts",
        F.col("_key").alias("kafka_key"),
        F.col("_reason").alias("reason"),
        F.col("_value_prefix").alias("value_prefix"),
    )


def merge_sql(target: str, view: str, keys: tuple[str, ...] = KEY_COLUMNS) -> str:
    """Sinh MERGE vào bảng đích từ view tạm, chỉ chèn khi chưa có khóa Kafka – replay không trùng dòng."""
    on = " AND ".join(f"t.{c} = s.{c}" for c in keys)
    return f"""MERGE INTO {target} t
USING {view} s
ON {on}
WHEN NOT MATCHED THEN INSERT *"""


def dedupe_batch(
    rows: DataFrame, columns: list[str], keys: tuple[str, ...] = KEY_COLUMNS
) -> DataFrame:
    """Giữ một dòng cho mỗi khóa Kafka trong batch, ưu tiên kafka_offset lớn nhất rồi ingest_ts mới nhất."""
    latest = Window.partitionBy(*keys).orderBy(
        F.col("kafka_offset").desc(), F.col("ingest_ts").desc()
    )
    return (
        rows.withColumn("_dedupe_rn", F.row_number().over(latest))
        .filter(F.col("_dedupe_rn") == 1)
        .drop("_dedupe_rn")
        .select(*columns)
    )


def kafka_end_offsets(
    bootstrap: str, timeout: float = 10.0
) -> dict[tuple[str, int], int]:
    """Lấy end offset của mọi partition khớp TOPIC_PATTERN bằng confluent-kafka AdminClient."""
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec

    pattern = re.compile(TOPIC_PATTERN)
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=timeout)
    topics = [name for name in metadata.topics if pattern.fullmatch(name)]
    requests = {
        TopicPartition(topic, partition): OffsetSpec.latest()
        for topic in topics
        for partition in metadata.topics[topic].partitions
    }
    if not requests:
        return {}
    futures = admin.list_offsets(requests, request_timeout=timeout)
    return {
        (tp.topic, tp.partition): int(future.result().offset)
        for tp, future in futures.items()
    }


def audit_rows(
    end_offsets: dict[tuple[str, int], int],
    bronze_max_offsets: dict[tuple[str, int], int],
    measured_at: datetime,
) -> list[tuple[str, int, int, int | None, int, datetime]]:
    """Tính dòng kiểm toán; end offset là offset tiếp theo sẽ ghi nên phải trừ 1 khi so sánh."""
    rows = []
    for (topic, partition), end in sorted(end_offsets.items()):
        bronze_max = bronze_max_offsets.get((topic, partition))
        covered = -1 if bronze_max is None else bronze_max
        rows.append(
            (
                topic,
                int(partition),
                int(end),
                bronze_max,
                max(int(end) - 1 - covered, 0),
                measured_at,
            )
        )
    return rows


def write_offset_audit(
    spark: SparkSession, bootstrap: str, measured_at: datetime | None = None
) -> int:
    """Ghi mỗi batch một dòng kiểm toán cho mỗi partition; lỗi AdminClient chỉ cảnh báo, không dừng job."""
    try:
        ends = kafka_end_offsets(bootstrap)
    except (KafkaException, OSError, ValueError) as exc:
        log.warning("Không lấy được end offset Kafka để kiểm toán: %s", exc)
        return 0
    if not ends:
        log.warning(
            "Không có partition nào khớp %s để kiểm toán offset; bỏ qua batch này",
            TOPIC_PATTERN,
        )
        return 0
    topics = ", ".join(
        "'" + topic.replace("'", "''") + "'" for topic in sorted({t for t, _ in ends})
    )
    current = spark.sql(f"""
        SELECT kafka_topic, kafka_partition, max(kafka_offset) AS max_offset
        FROM {TABLE}
        WHERE kafka_topic IN ({topics})
        GROUP BY 1, 2
    """).collect()
    bronze_max = {(r[0], int(r[1])): int(r[2]) for r in current if r[2] is not None}
    at = measured_at or datetime.now(timezone.utc).replace(tzinfo=None)
    rows = audit_rows(ends, bronze_max, at)
    if rows:
        spark.createDataFrame(rows, AUDIT_SCHEMA).writeTo(AUDIT_TABLE).append()
    return len(rows)


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
    bootstrap = os.environ["JOBLENS_KAFKA_BOOTSTRAP"]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(DDL)
    spark.sql(ERRORS_DDL)
    spark.sql(AUDIT_DDL)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
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
        try:
            good = dedupe_batch(batch.filter(F.col("_valid")), BRONZE_COLUMNS)
            bad_rows = dedupe_batch(errors(batch), ERROR_COLUMNS)
            n = good.count()
            bad = bad_rows.count()
            session = batch.sparkSession
            if n:
                good.createOrReplaceTempView("bronze_jobs_good")
                session.sql(merge_sql(TABLE, "bronze_jobs_good"))
            if bad:
                bad_rows.createOrReplaceTempView("bronze_jobs_bad")
                session.sql(merge_sql(ERRORS_TABLE, "bronze_jobs_bad"))
            audited = write_offset_audit(session, bootstrap)
            log.info(
                "batch %s: MERGE %s dòng bronze, %s dòng lỗi vào ingest_errors; kiểm toán %s partition",
                batch_id,
                n,
                bad,
                audited,
            )
        finally:
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
