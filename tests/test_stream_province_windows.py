"""Kiểm thử job streaming có trạng thái (spark-jobs/stream_province_windows.py) bằng Spark local:
aggregation theo cửa sổ, document MongoDB idempotent, và watermark loại tin đến trễ trên luồng thật (file source)."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark-jobs"))
from pyspark.sql import SparkSession  # noqa: E402

import stream_province_windows as S  # noqa: E402

KAFKA_COLS = "key string, value string, topic string, partition int, offset long"


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s


def msg(
    i: int,
    ts: str,
    place: str = "hà nội",
    salary: str = "10 - 20 triệu",
    offset: int | None = None,
):
    m = {
        "schema_version": "job_posting.v1",
        "source": "vietjobs",
        "source_job_id": str(i),
        "url": None,
        "crawl_ts": ts,
        "lastmod": None,
        "content_hash": "h" * 64,
        "raw_html_uri": None,
        "payload": {"title": f"Job {i}", "locations": [place], "salary_text": salary},
    }
    return {
        "key": str(i),
        "value": json.dumps(m, ensure_ascii=False),
        "topic": "jobs.raw.vietjobs.v1",
        "partition": 0,
        "offset": offset if offset is not None else i,
    }


def test_windowed_counts_batch(spark):
    rows = [
        msg(1, "2026-10-23T10:01:00.000Z"),
        msg(2, "2026-10-23T10:03:00.000Z", salary="Thỏa thuận"),
        msg(3, "2026-10-23T10:04:59.000Z", place="hồ chí minh"),
        msg(4, "2026-10-23T10:06:00.000Z"),
        msg(5, "2026-10-23T10:07:00.000Z", place="san francisco"),
    ]
    ev = S.postings_events(spark.createDataFrame(rows, KAFKA_COLS))
    out = {
        (r.window_start.strftime("%H:%M"), r.location_code): r
        for r in S.windowed_counts(ev, "5 minutes", "10 minutes").collect()
    }
    assert (
        out[("10:00", "01")].postings == 2 and out[("10:00", "01")].salary_postings == 1
    )
    assert out[("10:00", "01")].salary_mid_sum_vnd == 15_000_000
    assert out[("10:00", "79")].postings == 1
    assert out[("10:05", "01")].postings == 1
    assert (
        out[("10:05", "unknown")].postings == 1
    )  # không ánh xạ được tỉnh → "unknown", không mất tin


def test_documents_are_idempotent():
    start = datetime(2026, 10, 23, 10, 0, tzinfo=timezone.utc)
    row = {
        "window_start": start,
        "window_end": start,
        "location_code": "01",
        "postings": 3,
        "salary_postings": 2,
        "salary_mid_sum_vnd": 30_000_000.0,
        "sources": 1,
    }
    now = datetime(2026, 10, 23, 10, 9, tzinfo=timezone.utc)
    a = S.to_documents([row], 7, now)[0]
    b = S.to_documents([row], 8, now)[0]
    assert (
        a["_id"] == b["_id"] == "2026-10-23T10:00:00Z|01"
    )  # cùng khóa → ReplaceOne ghi đè, không nhân đôi
    assert a["avg_salary_mid_vnd"] == 15_000_000 and a["postings"] == 3
    assert (
        S.to_documents(
            [{**row, "salary_postings": 0, "salary_mid_sum_vnd": None}], 1, now
        )[0]["avg_salary_mid_vnd"]
        is None
    )


def test_watermark_drops_late_events_on_a_real_stream(spark, tmp_path):
    src, ckpt = tmp_path / "in", tmp_path / "ckpt"
    src.mkdir()
    seen: dict[tuple[str, str], int] = {}

    def sink(batch, _id):
        for r in batch.collect():
            seen[(r.window_start.strftime("%H:%M"), r.location_code)] = r.postings

    stream = spark.readStream.schema(KAFKA_COLS).json(str(src))
    q = (
        S.windowed_counts(S.postings_events(stream), "5 minutes", "10 minutes")
        .writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", str(ckpt))
        .start()
    )

    def feed(name, rows):
        (src / name).write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
        )
        q.processAllAvailable()

    try:
        feed("a.json", [msg(1, "2026-10-23T10:01:00Z"), msg(2, "2026-10-23T10:30:00Z")])
        # watermark = 10:30 − 10 phút = 10:20 → tin 10:02 (cửa sổ 10:00 đã đóng) bị bỏ, tin 10:26 được nhận
        feed("b.json", [msg(3, "2026-10-23T10:02:00Z"), msg(4, "2026-10-23T10:26:00Z")])
        dropped = sum(
            op["numRowsDroppedByWatermark"] for op in q.lastProgress["stateOperators"]
        )
    finally:
        q.stop()
    assert seen[("10:00", "01")] == 1  # vẫn 1, tin trễ không được cộng
    assert seen[("10:25", "01")] == 1 and seen[("10:30", "01")] == 1
    assert dropped == 1
