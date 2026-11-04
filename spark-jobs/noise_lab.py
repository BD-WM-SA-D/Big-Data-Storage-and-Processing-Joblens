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
Bước `evaluate` (phần 3): so kết quả với đáp án và với lượt sạch `r0_s42` CÙNG chế độ (mốc), ghi
  lake.lab.noise_metrics   theo lượt × chế độ × loại nhiễu: số tin, số xử lý đúng, tỉ lệ
  lake.lab.gold_impact     nhiễu làm lệch trung vị lương / số tin theo tỉnh bao nhiêu so với mốc sạch
  lake.lab.clean_diffs     trên dữ liệu SẠCH, tin nào robust cho kết quả khác baseline (để kiểm tra nhầm)
       EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab evaluate
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
METRICS = f"{NS}.noise_metrics"
GOLD_IMPACT = f"{NS}.gold_impact"
CLEAN_DIFFS = f"{NS}.clean_diffs"
REFERENCE_RUN = "r0_s42"
MIN_SALARY_JOBS = (
    200  # như ô Grafana "Lương trung vị theo tỉnh": chỉ tỉnh có ≥ 200 tin có lương
)
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


def judge(truth, postings, quarantine, reference_run: str = REFERENCE_RUN):
    """Mỗi dòng đáp án × mỗi chế độ → cột `ok` (pipeline xử lý đúng loại nhiễu đó hay không).

    Mốc so sánh: kết quả của CÙNG chế độ trên lượt sạch (reference_run) – để chỉ đo ảnh hưởng của nhiễu, không lẫn
    khác biệt giữa hai cách chuẩn hóa.
    """
    from pyspark.sql import functions as F

    modes = postings.sparkSession.createDataFrame([(m,) for m in MODES], "mode string")
    cols = [
        "title",
        "salary_min_vnd",
        "salary_max_vnd",
        "location_codes",
        "experience_min_years",
    ]
    ref = postings.filter(F.col("run_id") == reference_run).select(
        "mode", "source_job_id", *[F.col(c).alias(f"ref_{c}") for c in cols]
    )
    out = postings.select(
        "run_id", "mode", "source_job_id", F.lit(True).alias("present"), *cols
    )
    q = quarantine.select(
        "run_id", "mode", F.col("kafka_key").alias("source_job_id")
    ).distinct()
    q = q.withColumn("quarantined", F.lit(True))
    j = (
        truth.crossJoin(modes)
        .join(out, ["run_id", "mode", "source_job_id"], "left")
        .join(ref, ["mode", "source_job_id"], "left")
        .join(q, ["run_id", "mode", "source_job_id"], "left")
    )
    present = F.coalesce(F.col("present"), F.lit(False))
    salary_eq = F.col("salary_min_vnd").eqNullSafe(F.col("ref_salary_min_vnd")) & F.col(
        "salary_max_vnd"
    ).eqNullSafe(F.col("ref_salary_max_vnd"))
    loc_eq = F.col("location_codes").eqNullSafe(F.col("ref_location_codes"))
    title_eq = F.col("title").eqNullSafe(F.col("ref_title"))
    no_salary = F.col("salary_min_vnd").isNull() & F.col("salary_max_vnd").isNull()
    e, f = F.col("expected"), F.col("field")
    ok = (
        F.when((e == "recover") & (f == "salary"), present & salary_eq)
        .when((e == "recover") & (f == "location"), present & loc_eq)
        .when((e == "recover") & (f == "title"), present & title_eq)
        .when(
            (e == "recover"), present & title_eq & loc_eq & salary_eq
        )  # title+location+salary
        .when((e == "flag") & (f == "salary"), present & no_salary)
        .when((e == "flag"), present & F.col("experience_min_years").isNull())
        .when((e == "unknown") & (f == "salary"), present & no_salary)
        .when((e == "unknown"), present & no_salary & (F.size("location_codes") == 0))
        .when(e.isin("dedup", "keep_latest"), present & title_eq & salary_eq)
        .when(
            e == "quarantine", ~present & F.coalesce(F.col("quarantined"), F.lit(False))
        )
        .otherwise(F.lit(False))
    )
    return j.withColumn("ok", F.coalesce(ok, F.lit(False)))


def metrics(judged):
    from pyspark.sql import functions as F

    return (
        judged.groupBy("run_id", "mode", "noise_type", "expected")
        .agg(F.count("*").alias("total"), F.sum(F.col("ok").cast("long")).alias("ok"))
        .withColumn("rate", F.round(F.col("ok") / F.col("total"), 4))
    )


def province_stats(postings):
    """Theo lượt × chế độ × tỉnh: số tin, số tin có lương, trung vị lương giữa khoảng (VND/tháng)."""
    from pyspark.sql import functions as F

    mid = (
        F.coalesce("salary_min_vnd", "salary_max_vnd")
        + F.coalesce("salary_max_vnd", "salary_min_vnd")
    ) / 2
    return (
        postings.select(
            "run_id",
            "mode",
            F.explode("location_codes").alias("location_code"),
            mid.alias("mid"),
        )
        .groupBy("run_id", "mode", "location_code")
        .agg(
            F.count("*").alias("jobs"),
            F.count("mid").alias("salary_jobs"),
            F.median("mid").alias("median_salary"),
        )
    )


def gold_impact(
    postings, reference_run: str = REFERENCE_RUN, min_salary_jobs: int = MIN_SALARY_JOBS
):
    """Sai lệch so với mốc sạch cùng chế độ: trung bình / lớn nhất % lệch trung vị lương, % lệch số tin theo tỉnh."""
    from pyspark.sql import functions as F

    st = province_stats(postings)
    ref = st.filter(F.col("run_id") == reference_run).select(
        "mode", "location_code", F.col("jobs").alias("ref_jobs"),
        F.col("salary_jobs").alias("ref_salary_jobs"), F.col("median_salary").alias("ref_median"),
    )  # fmt: skip
    j = st.join(ref, ["mode", "location_code"]).filter(
        F.col("ref_salary_jobs") >= min_salary_jobs
    )
    ape_med = F.abs(F.col("median_salary") - F.col("ref_median")) / F.col("ref_median")
    ape_jobs = F.abs(F.col("jobs") - F.col("ref_jobs")) / F.col("ref_jobs")
    return (
        j.groupBy("run_id", "mode")
        .agg(
            F.count("*").alias("provinces"),
            F.round(F.avg(ape_med) * 100, 3).alias("median_salary_mape_pct"),
            F.round(F.max(ape_med) * 100, 3).alias("median_salary_max_ape_pct"),
            F.round(F.avg(ape_jobs) * 100, 3).alias("job_count_mape_pct"),
        )
        .orderBy("run_id", "mode")
    )


def clean_diffs(postings, messages, reference_run: str = REFERENCE_RUN):
    """Trên lượt sạch: tin nào chế độ robust cho kết quả khác baseline, kèm chuỗi gốc – để người kiểm tra xem
    là sửa đúng hay nhầm (dữ liệu sạch không có đáp án)."""
    from pyspark.sql import functions as F

    base = postings.filter(
        (F.col("run_id") == reference_run) & (F.col("mode") == "baseline")
    ).alias("b")
    rob = postings.filter(
        (F.col("run_id") == reference_run) & (F.col("mode") == "robust")
    ).alias("r")
    raw = messages.filter(F.col("run_id") == reference_run).select(
        F.col("kafka_key").alias("source_job_id"),
        F.get_json_object("value", "$.payload.locations").alias("raw_locations"),
        F.get_json_object("value", "$.payload.salary_text").alias("raw_salary"),
        F.get_json_object("value", "$.payload.experience_text").alias("raw_experience"),
    )
    j = base.join(rob, "source_job_id").join(raw, "source_job_id", "left")
    checks = [
        ("location", "location_codes", "raw_locations"),
        ("salary", "salary_min_vnd", "raw_salary"),
        ("salary", "salary_max_vnd", "raw_salary"),
        ("experience", "experience_min_years", "raw_experience"),
    ]
    parts = [
        j.filter(~F.col(f"b.{c}").eqNullSafe(F.col(f"r.{c}"))).select(
            "source_job_id",
            F.lit(name).alias("field"),
            F.col(f"b.{c}").cast("string").alias("baseline_value"),
            F.col(f"r.{c}").cast("string").alias("robust_value"),
            F.col(raw_col).alias("raw_value"),
        )
        for name, c, raw_col in checks
    ]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.dropDuplicates(["source_job_id", "field"])


def _table_md(rows, cols) -> str:
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += [
        "| " + " | ".join("" if r[c] is None else str(r[c]) for c in cols) + " |"
        for r in rows
    ]
    return "\n".join(lines)


def evaluate(spark, args) -> None:
    from pyspark.sql import functions as F

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    postings = spark.table(POSTINGS).persist()
    truth = spark.table(TRUTH)
    judged = judge(truth, postings, spark.table(QUARANTINE), args.reference)
    m = metrics(judged).persist()
    m.writeTo(METRICS).using("iceberg").createOrReplace()
    gi = gold_impact(postings, args.reference)
    gi.writeTo(GOLD_IMPACT).using("iceberg").createOrReplace()
    cd = clean_diffs(postings, spark.table(MESSAGES), args.reference)
    cd.writeTo(CLEAN_DIFFS).using("iceberg").createOrReplace()

    # bảng tóm tắt theo loại nhiễu (gộp mọi lượt nhiễu) – in ra log để chép vào benchmarks/noise_robustness.md
    by_type = (
        m.groupBy("noise_type", "expected")
        .pivot("mode", list(MODES))
        .agg((F.sum("ok") / F.sum("total")).alias("r"), F.sum("total").alias("n"))
        .orderBy("noise_type")
        .collect()
    )
    rows = [
        {
            "loại nhiễu": r["noise_type"],
            "mong đợi": r["expected"],
            "số tin": r["robust_n"],
            "baseline": f"{r['baseline_r'] * 100:.1f}%"
            if r["baseline_r"] is not None
            else "",
            "robust": f"{r['robust_r'] * 100:.1f}%"
            if r["robust_r"] is not None
            else "",
        }
        for r in by_type
    ]
    log.info(
        "Tỉ lệ xử lý đúng theo loại nhiễu (gộp các lượt):\n%s",
        _table_md(rows, list(rows[0]) if rows else []),
    )
    overall = (
        m.groupBy("run_id", "mode")
        .agg(
            F.sum("total").alias("tin_nhieu"),
            F.round(F.sum("ok") / F.sum("total") * 100, 2).alias("dung_pct"),
        )
        .orderBy("run_id", "mode")
        .collect()
    )
    log.info(
        "Theo lượt:\n%s",
        _table_md(overall, ["run_id", "mode", "tin_nhieu", "dung_pct"]),
    )
    gir = spark.table(GOLD_IMPACT).collect()
    log.info("Lệch so với mốc sạch (tỉnh ≥ %s tin có lương):\n%s", MIN_SALARY_JOBS, _table_md(
        gir, ["run_id", "mode", "provinces", "median_salary_mape_pct", "median_salary_max_ape_pct", "job_count_mape_pct"]
    ))  # fmt: skip
    diffs = spark.table(CLEAN_DIFFS).groupBy("field").count().orderBy("field").collect()
    log.info(
        "Dữ liệu sạch – số tin robust khác baseline: %s",
        {r["field"]: r["count"] for r in diffs},
    )
    postings.unpersist()


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
    pe = sub.add_parser("evaluate", help="Phần 3: đo so với đáp án và mốc sạch")
    pe.add_argument("--reference", default=REFERENCE_RUN)
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
    elif args.step == "evaluate":
        evaluate(spark, args)


if __name__ == "__main__":
    main()
