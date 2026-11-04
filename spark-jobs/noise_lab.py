"""Thí nghiệm nhiễu (docs/thi-nghiem-nhieu.md) – job Spark chạy trong namespace Iceberg riêng `lab`, không đụng bảng thật.

Bước `inject` (phần 1): đọc VietJobs → message job_posting.v1 sạch (cùng hàm với vietjobs.py) → tiêm nhiễu
(noise.py) → ghi
  lake.lab.noisy_messages  message gửi đi theo thứ tự (giống value Kafka), có cả message hỏng
  lake.lab.noise_truth     đáp án: tin nào bị nhiễu gì, giá trị sạch, hành vi mong đợi
  lake.lab.noise_runs      mỗi lượt một dòng: tỉ lệ, seed, số tin, số tin nhiễu theo loại
Mỗi lượt có run_id (mặc định r<tỉ lệ %>_s<seed>), chạy lại cùng run_id thì ghi đè đúng phần của lượt đó.

Chạy:  EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh noise_lab inject --rate 0.2 --seed 42
       (thêm --limit 5000 để thử nhanh)
Các bước sau (xử lý nhiễu, đo) thêm vào job này ở phần 2, 3.
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
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    import bronze_jobs

    spark = bronze_jobs.build_session(f"noise-lab-{args.step}")
    if args.step == "inject":
        inject(spark, args)


if __name__ == "__main__":
    main()
