"""Nạp VietJobs (Pham Dinh et al., LREC 2026 – https://github.com/VinNLP/VietJobs, MIT) vào nền tảng.

Hai chế độ (docs/project-plan/03, thí nghiệm batch vs streaming):
  --sink bronze   Đường A – batch: ghi thẳng vào Iceberg bronze.jobs_raw (kafka_topic = 'backfill.vietjobs').
                  Chạy bằng Spark: bash deploy/spark/submit.sh vietjobs --sink bronze
  --sink kafka    Replay: gửi từng đợt vào topic jobs.raw.vietjobs.v1 với tốc độ --rate tin/giây, để job
                  streaming bronze_jobs nạp tiếp (đường B) – dùng cho demo và đo tải. Đóng vai nguồn sự kiện
                  (như crawler) nên KHÔNG cần Spark: producer Python (confluent-kafka) chạy trong một pod nhỏ –
                  bash deploy/data/replay-vietjobs.sh (~300 MB RAM thay vì driver + executor Spark ~2,5 GB).

File CSV gốc (18 cột: job_title, location, country, qualifications, technical_skills, soft_skills,
languages_required, experience_required, salary, contract_type, working_hours, benefits, description,
requirements_text, category, salary_min, salary_max, salary_avg) nằm trên s3://joblens/raw/vietjobs/ –
tải lên bằng deploy/data/upload-vietjobs.sh. Không commit dữ liệu vào git.

source_job_id = số thứ tự dòng (0-based) trong CSV gốc – cùng quy ước với Web-mining-joblens/ie/vietjobs.py,
nên job_uid của hai repo khớp nhau. VietJobs không có công ty, ngày đăng, URL, cấp bậc → để null.
Cột technical_skills do LLM trích (nhãn yếu), không phải tag của nhà tuyển dụng → không đưa vào skill_tags.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import io
import itertools
import json
import logging
import os
import sys
import time
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone

SOURCE = "vietjobs"
BACKFILL_TOPIC = "backfill.vietjobs"
REPLAY_TOPIC = "jobs.raw.vietjobs.v1"
EXPECTED_COLUMNS = [
    "job_title", "location", "country", "qualifications", "technical_skills", "soft_skills",
    "languages_required", "experience_required", "salary", "contract_type", "working_hours", "benefits",
    "description", "requirements_text", "category", "salary_min", "salary_max", "salary_avg",
]  # fmt: skip

log = logging.getLogger("vietjobs")


def to_list(value) -> list[str]:
    """Cột danh sách lưu dạng chuỗi "['a', 'b']" → list."""
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        out = ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return [s.strip() for s in value.split(",") if s.strip()]
    if isinstance(out, (list, tuple)):
        return [str(x).strip() for x in out if str(x).strip()]
    return [str(out).strip()]


def _blank(value) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def to_message(row: dict, index: int, crawl_ts: str) -> dict:
    """Một dòng CSV VietJobs → message job_posting.v1 (schema joblens/schemas/kafka/job_posting.v1.json)."""
    benefits = to_list(row.get("benefits"))
    payload = {
        "title": (row.get("job_title") or "").strip(),
        "company_name": None,
        "company_id": None,
        "salary_text": _blank(row.get("salary")),
        "locations": [
            p.strip() for p in (row.get("location") or "").split(",") if p.strip()
        ],
        "experience_text": _blank(row.get("experience_required")),
        "level": None,
        "employment_type": _blank(row.get("contract_type")),
        "category": _blank(row.get("category")),
        "deadline": None,
        "date_posted": None,
        "skill_tags": [],
        "description": _blank(row.get("description")),
        "requirements": _blank(row.get("requirements_text")),
        "benefits": "; ".join(benefits) if benefits else None,
    }
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return {
        "schema_version": "job_posting.v1",
        "source": SOURCE,
        "source_job_id": str(index),
        "url": None,
        "crawl_ts": crawl_ts,
        "lastmod": None,
        "content_hash": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "raw_html_uri": None,
        "payload": payload,
    }


def read_rows(text_stream: Iterable[str]) -> Iterator[tuple[int, dict]]:
    """Đọc CSV VietJobs (có trường nhiều dòng), trả (số thứ tự dòng, dict). Bỏ dòng không có tiêu đề."""
    reader = csv.DictReader(text_stream)
    missing = [
        c
        for c in ("job_title", "location", "salary", "description")
        if c not in (reader.fieldnames or [])
    ]
    if missing:
        raise ValueError(f"CSV thiếu cột {missing} – không phải file VietJobs?")
    for i, row in enumerate(reader):
        if (row.get("job_title") or "").strip():
            yield i, row


def _download_s3(uri: str) -> io.TextIOWrapper:
    import boto3  # có trong image spark-jobs
    from botocore.config import Config

    bucket, key = uri.removeprefix("s3://").split("/", 1)
    # SeaweedFS: bắt buộc path-style (http://host/bucket/key); kiểu mặc định đưa bucket vào tên miền
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["JOBLENS_S3_ENDPOINT"],
        config=Config(s3={"addressing_style": "path"}),
    )
    local = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket, key, local)
    return open(local, encoding="utf-8", newline="")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description="Nạp VietJobs vào bronze (batch) hoặc replay vào Kafka"
    )
    p.add_argument("--input", default="s3://joblens/raw/vietjobs/VietJobs.csv")
    p.add_argument("--sink", choices=["bronze", "kafka"], default="bronze")
    p.add_argument(
        "--limit", type=int, default=0, help="Chỉ lấy N tin đầu (0 = tất cả)"
    )
    p.add_argument("--rate", type=float, default=50.0, help="Kafka: số tin/giây")
    p.add_argument("--batch", type=int, default=500, help="Kafka: số tin mỗi lần gửi")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    crawl_ts = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )  # VietJobs không có ngày: dùng thời điểm nạp
    stream = (
        _download_s3(args.input)
        if args.input.startswith("s3://")
        else open(args.input, encoding="utf-8", newline="")
    )
    with (
        stream
    ):  # cắt --limit ngay khi đọc để pod replay không giữ cả 48k tin trong RAM
        rows = itertools.islice(read_rows(stream), args.limit or None)
        msgs = [to_message(r, i, crawl_ts) for i, r in rows]
    log.info("đọc %s tin VietJobs từ %s", len(msgs), args.input)

    if args.sink == "kafka":
        replay_to_kafka(
            msgs, os.environ["JOBLENS_KAFKA_BOOTSTRAP"], args.rate, args.batch
        )
        return

    from pyspark.sql import functions as F

    import bronze_jobs

    spark = bronze_jobs.build_session(f"vietjobs-{args.sink}")
    if args.sink == "bronze":
        rows = [
            (
                BACKFILL_TOPIC,
                -1,
                int(m["source_job_id"]),
                m["source_job_id"].encode(),
                json.dumps(m, ensure_ascii=False).encode(),
            )
            for m in msgs
        ]
        df = spark.createDataFrame(
            rows, "topic string, partition int, offset long, key binary, value binary"
        )
        out = (
            bronze_jobs.to_bronze(df)
            .filter(F.col("_valid"))
            .select(*bronze_jobs.BRONZE_COLUMNS)
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {bronze_jobs.CATALOG}.bronze")
        spark.sql(bronze_jobs.DDL)
        out.writeTo(bronze_jobs.TABLE).append()
        log.info("đã ghi %s dòng vào %s", out.count(), bronze_jobs.TABLE)
        return


def replay_to_kafka(
    msgs: list[dict],
    bootstrap: str,
    rate: float,
    batch: int,
    producer=None,
    sleep=time.sleep,
    event_time: bool = True,
    now_iso=lambda: datetime.now(timezone.utc)
    .isoformat(timespec="milliseconds")
    .replace("+00:00", "Z"),
) -> int:
    """Gửi message vào REPLAY_TOPIC theo đợt `batch` tin, giữ tốc độ ~`rate` tin/giây. Trả số tin đã gửi.

    key = source_job_id → cùng một tin luôn vào cùng partition (thứ tự theo tin). acks=all + idempotence:
    gửi lại khi lỗi mạng không sinh bản trùng trong Kafka. event_time=True: crawl_ts = lúc gửi (độ chính xác ms),
    content_hash giữ nguyên vì không tính crawl_ts.
    """
    if producer is None:
        from confluent_kafka import Producer

        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )
    failed: list[str] = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            failed.append(str(err))

    sent = 0
    for start in range(0, len(msgs), batch):
        t0 = time.monotonic()
        chunk = msgs[start : start + batch]
        for m in chunk:
            if event_time:
                # thời điểm phát sự kiện (như crawler vừa lấy tin) – dùng đo độ trễ đầu-cuối:
                # thời điểm dữ liệu đọc được trong bronze − crawl_ts (scripts/benchmark-batch-vs-streaming.sh)
                m = {**m, "crawl_ts": now_iso()}
            producer.produce(
                REPLAY_TOPIC,
                key=m["source_job_id"].encode(),
                value=json.dumps(m, ensure_ascii=False).encode(),
                on_delivery=on_delivery,
            )
            producer.poll(0)
        producer.flush(60)
        if failed:
            raise RuntimeError(f"Kafka từ chối {len(failed)} tin, ví dụ: {failed[0]}")
        sent += len(chunk)
        log.info("replay: đã gửi %s/%s tin vào %s", sent, len(msgs), REPLAY_TOPIC)
        wait = len(chunk) / rate - (time.monotonic() - t0)
        if wait > 0 and sent < len(msgs):
            sleep(wait)
    return sent


if __name__ == "__main__":
    sys.exit(main())
