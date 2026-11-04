"""Thí nghiệm nhiễu (docs/thi-nghiem-nhieu.md) – job Spark chạy trong namespace Iceberg riêng `lab`, không đụng bảng thật.

Bước `inject` (phần 1): đọc VietJobs → message job_posting.v1 sạch (cùng hàm với vietjobs.py) → tiêm nhiễu
(noise.py) → ghi
  lake.lab.noisy_messages  message gửi đi theo thứ tự (giống value Kafka), có cả message hỏng
  lake.lab.noise_truth     đáp án: tin nào bị nhiễu gì, giá trị sạch, hành vi mong đợi
  lake.lab.noise_runs      mỗi lượt một dòng: tỉ lệ, seed, số tin, số tin nhiễu theo loại
Mỗi lượt có run_id (mặc định r<tỉ lệ %>_s<seed>), chạy lại cùng run_id thì ghi đè đúng phần của lượt đó.

Bước `clean` (phần 2): cho message của từng lượt đi qua ĐÚNG mã của pipeline thật – to_bronze (tách message hỏng
kèm lý do), build_updates của silver (giữ bản crawl mới nhất mỗi job_uid, chuẩn hóa) – ở hai chế độ:
  baseline   chuẩn hóa như trước phần 2 (normalize robust=False), message hỏng bị bỏ im lặng
  robust     chuẩn hóa mới (làm sạch chuỗi, lương năm, tỉnh gõ sai, loại ngoại lai), message hỏng vào cách ly
và ghi
  lake.lab.job_postings    kết quả silver của lượt × chế độ
  lake.lab.quarantine      message hỏng kèm lý do (chế độ robust)

Chạy:  EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab inject --rate 0.2 --seed 42
       (thêm --limit 5000 để thử nhanh)
       EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab clean      # mọi lượt, cả hai chế độ
Bước đo (phần 3) thêm vào job này sau.
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
from datetime import datetime, timezone

import noise
import vietjobs

CATALOG = "lake"
NS = f"{CATALOG}.lab"
MESSAGES = f"{NS}.noisy_messages"
TRUTH = f"{NS}.noise_truth"
RUNS = f"{NS}.noise_runs"
POSTINGS = f"{NS}.job_postings"
QUARANTINE = f"{NS}.quarantine"
MODES = ("baseline", "robust")
POSTING_COLUMNS = [
    "run_id", "mode", "job_uid", "source_job_id", "crawl_ts", "title", "salary_min_vnd", "salary_max_vnd",
    "salary_is_negotiable", "location_codes", "experience_min_years", "level_code",
]  # fmt: skip

DDL = [
    f"""CREATE TABLE IF NOT EXISTS {MESSAGES} (
        run_id STRING NOT NULL, seq BIGINT NOT NULL, kafka_key STRING, value STRING
    ) USING iceberg PARTITIONED BY (run_id)
    TBLPROPERTIES ('format-version' = '2', 'comment' = 'Thí nghiệm nhiễu: message gửi đi theo thứ tự')""",
    f"""CREATE TABLE IF NOT EXISTS {TRUTH} (
        run_id STRING NOT NULL, source_job_id STRING NOT NULL, noise_type STRING NOT NULL, level STRING,
        field STRING, expected STRING, clean_value STRING, noisy_value STRING, extra_json STRING
    ) USING iceberg PARTITIONED BY (run_id)
    TBLPROPERTIES ('format-version' = '2', 'comment' = 'Thí nghiệm nhiễu: đáp án từng tin bị nhiễu')""",
    f"""CREATE TABLE IF NOT EXISTS {POSTINGS} (
        run_id STRING NOT NULL, mode STRING NOT NULL, job_uid STRING NOT NULL, source_job_id STRING NOT NULL,
        crawl_ts TIMESTAMP, title STRING, salary_min_vnd BIGINT, salary_max_vnd BIGINT,
        salary_is_negotiable BOOLEAN, location_codes ARRAY<STRING>, experience_min_years DOUBLE, level_code STRING
    ) USING iceberg PARTITIONED BY (run_id, mode)
    TBLPROPERTIES ('format-version' = '2', 'comment' = 'Thí nghiệm nhiễu: kết quả silver theo lượt và chế độ')""",
    f"""CREATE TABLE IF NOT EXISTS {QUARANTINE} (
        run_id STRING NOT NULL, mode STRING NOT NULL, seq BIGINT, kafka_key STRING, reason STRING,
        value_prefix STRING
    ) USING iceberg PARTITIONED BY (run_id, mode)
    TBLPROPERTIES ('format-version' = '2', 'comment' = 'Thí nghiệm nhiễu: message hỏng bị cách ly kèm lý do')""",
    f"""CREATE TABLE IF NOT EXISTS {RUNS} (
        run_id STRING NOT NULL, rate DOUBLE, seed INT, messages BIGINT, noisy BIGINT,
        by_type_json STRING, created_at TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2', 'comment' = 'Thí nghiệm nhiễu: các lượt đã chạy')""",
]

log = logging.getLogger("noise_lab")


def default_run_id(rate: float, seed: int) -> str:
    return f"r{round(rate * 100)}_s{seed}"


def build_rows(messages: list[dict], rate: float, seed: int, run_id: str):
    """(dòng noisy_messages, dòng noise_truth, tóm tắt) – thuần Python để kiểm thử không cần Spark."""
    emissions, truths = noise.inject(messages, rate, seed)
    msg_rows = [(run_id, e.seq, e.key, e.value) for e in emissions]
    truth_rows = [
        (
            run_id, t.source_job_id, t.noise_type, t.level, t.field, t.expected,
            t.clean_value, t.noisy_value, json.dumps(t.extra) if t.extra else None,
        )
        for t in truths
    ]  # fmt: skip
    return msg_rows, truth_rows, noise.summary(truths, len(messages))


def load_messages(input_uri: str, limit: int) -> list[dict]:
    crawl_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stream = (
        vietjobs._download_s3(input_uri)
        if input_uri.startswith("s3://")
        else open(input_uri, encoding="utf-8", newline="")
    )
    with stream:
        rows = itertools.islice(vietjobs.read_rows(stream), limit or None)
        return [vietjobs.to_message(r, i, crawl_ts) for i, r in rows]


def inject(spark, args) -> None:
    run_id = args.run_id or default_run_id(args.rate, args.seed)
    messages = load_messages(args.input, args.limit)
    msg_rows, truth_rows, summ = build_rows(messages, args.rate, args.seed, run_id)
    log.info("lượt %s: %s", run_id, json.dumps(summ, ensure_ascii=False))

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    for ddl in DDL:
        spark.sql(ddl)
    spark.createDataFrame(
        msg_rows, "run_id string, seq long, kafka_key string, value string"
    ).writeTo(MESSAGES).overwritePartitions()
    truth_schema = (
        "run_id string, source_job_id string, noise_type string, level string, field string, "
        "expected string, clean_value string, noisy_value string, extra_json string"
    )
    if truth_rows:
        spark.createDataFrame(truth_rows, truth_schema).writeTo(
            TRUTH
        ).overwritePartitions()
    else:  # rate = 0: xóa đáp án cũ của run_id này
        spark.sql(f"DELETE FROM {TRUTH} WHERE run_id = '{run_id}'")
    spark.sql(f"DELETE FROM {RUNS} WHERE run_id = '{run_id}'")
    spark.createDataFrame(
        [
            (
                run_id,
                float(args.rate),
                int(args.seed),
                summ["messages"],
                summ["noisy"],
                json.dumps(summ["by_type"]),
                datetime.now(timezone.utc),
            )
        ],  # fmt: skip
        "run_id string, rate double, seed int, messages long, noisy long, by_type_json string, created_at timestamp",
    ).writeTo(RUNS).append()
    log.info(
        "đã ghi %s message (%s tin nhiễu) vào %s, đáp án vào %s",
        len(msg_rows), summ["noisy"], MESSAGES, TRUTH,
    )  # fmt: skip
    for k, v in summ["by_type"].items():
        log.info("  %-20s %6d", k, v)


def as_kafka(messages):
    """lab.noisy_messages → DataFrame giống nguồn Kafka (topic, partition, offset = seq, key, value)."""
    from pyspark.sql import functions as F

    return messages.select(
        F.lit("lab.noise").alias("topic"),
        F.lit(0).alias("partition"),
        F.col("seq").alias("offset"),
        F.col("kafka_key").cast("binary").alias("key"),
        F.col("value").cast("binary").alias("value"),
    )


def process(messages, run_id: str, mode: str, now: datetime):
    """Một lượt × một chế độ → (DataFrame lab.job_postings, DataFrame lab.quarantine). Dùng lại mã pipeline thật."""
    from pyspark.sql import functions as F

    import bronze_jobs
    import silver_job_postings as S

    rows = bronze_jobs.to_bronze(
        as_kafka(messages), ingest_ts=F.lit(now).cast("timestamp")
    )
    good = rows.filter(F.col("_valid")).select(*bronze_jobs.BRONZE_COLUMNS)
    silver = S.build_updates(
        good, now=now, normalizer=S.make_normalize_udf(mode == "robust")
    )
    postings = silver.select(
        F.lit(run_id).alias("run_id"),
        F.lit(mode).alias("mode"),
        "job_uid",
        "source_job_id",
        F.col("last_seen_ts").alias("crawl_ts"),
        "title",
        "salary_min_vnd",
        "salary_max_vnd",
        "salary_is_negotiable",
        "location_codes",
        "experience_min_years",
        "level_code",
    )
    bad = bronze_jobs.errors(rows)
    if mode == "baseline":  # pipeline cũ bỏ im lặng – không có gì để cách ly
        bad = bad.limit(0)
    quarantine = bad.select(
        F.lit(run_id).alias("run_id"),
        F.lit(mode).alias("mode"),
        F.col("kafka_offset").alias("seq"),
        "kafka_key",
        "reason",
        "value_prefix",
    )
    return postings, quarantine


def clean(spark, args) -> None:
    from pyspark.sql import functions as F

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    for ddl in DDL:
        spark.sql(ddl)
    runs = (
        args.runs.split(",")
        if args.runs
        else [
            r.run_id
            for r in spark.table(RUNS).select("run_id").orderBy("run_id").collect()
        ]
    )
    modes = args.modes.split(",")
    now = datetime.now(timezone.utc)
    for run_id in runs:
        messages = spark.table(MESSAGES).filter(F.col("run_id") == run_id).persist()
        if messages.isEmpty():
            log.warning("lượt %s không có message – bỏ qua (chạy inject trước)", run_id)
            continue
        for mode in modes:
            postings, quarantine = process(messages, run_id, mode, now)
            postings.writeTo(POSTINGS).overwritePartitions()
            spark.sql(
                f"DELETE FROM {QUARANTINE} WHERE run_id = '{run_id}' AND mode = '{mode}'"
            )
            quarantine.writeTo(QUARANTINE).append()
            stats = (
                spark.table(POSTINGS)
                .filter((F.col("run_id") == run_id) & (F.col("mode") == mode))
                .agg(
                    F.count("*").alias("tin"),
                    F.count("salary_min_vnd").alias("co_luong"),
                    F.sum(F.when(F.size("location_codes") > 0, 1).otherwise(0)).alias(
                        "co_tinh"
                    ),
                )
                .first()
            )
            q = (
                spark.table(QUARANTINE)
                .filter((F.col("run_id") == run_id) & (F.col("mode") == mode))
                .count()
            )
            log.info(
                "lượt %s, chế độ %-8s: %s tin silver, %s có lương, %s có tỉnh, %s message cách ly",
                run_id, mode, stats["tin"], stats["co_luong"], stats["co_tinh"], q,
            )  # fmt: skip
        messages.unpersist()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Thí nghiệm nhiễu trên namespace lab")
    sub = p.add_subparsers(dest="step", required=True)
    pi = sub.add_parser("inject", help="Phần 1: tạo dữ liệu nhiễu + đáp án")
    pi.add_argument("--input", default="s3://joblens/raw/vietjobs/VietJobs.csv")
    pi.add_argument(
        "--limit", type=int, default=0, help="Chỉ lấy N tin đầu (0 = tất cả)"
    )
    pi.add_argument("--rate", type=float, default=0.2, help="Tỉ lệ tin bị nhiễu (0–1)")
    pi.add_argument("--seed", type=int, default=42)
    pi.add_argument("--run-id", default=None)
    pc = sub.add_parser(
        "clean", help="Phần 2: chạy pipeline baseline và robust trên dữ liệu nhiễu"
    )
    pc.add_argument(
        "--runs",
        default=None,
        help="Danh sách run_id cách nhau dấu phẩy (mặc định: mọi lượt)",
    )
    pc.add_argument("--modes", default=",".join(MODES))
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    import bronze_jobs

    spark = bronze_jobs.build_session(f"noise-lab-{args.step}")
    if args.step == "inject":
        inject(spark, args)
    elif args.step == "clean":
        clean(spark, args)


if __name__ == "__main__":
    main()
