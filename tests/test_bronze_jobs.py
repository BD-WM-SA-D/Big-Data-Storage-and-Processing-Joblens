"""Kiểm thử phép biến đổi Kafka → bronze.jobs_raw bằng Spark cục bộ (không cần Kafka, Iceberg hay cụm K8s).

Đầu vào là fixtures job_posting.v1 của repo joblens, giả lập đúng các cột mà nguồn Kafka của Spark trả về.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from joblens_contracts import fixture_path, load_table_contract

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark-jobs"))
import bronze_jobs  # noqa: E402

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

CONTRACT_TYPES = {
    "string": "string",
    "int": "int",
    "long": "bigint",
    "timestamp": "timestamp",
}


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("test-bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def kafka_rows(records, topic="jobs.raw.topcv.v1"):
    """Giả lập DataFrame nguồn Kafka: key/value là bytes, kèm topic/partition/offset."""
    return [
        (
            r.get("source_job_id", "").encode() if isinstance(r, dict) else None,
            (json.dumps(r, ensure_ascii=False) if isinstance(r, dict) else r).encode(),
            topic,
            i % 6,
            i,
        )
        for i, r in enumerate(records)
    ]


def make_df(spark, rows):
    return spark.createDataFrame(
        rows, "key binary, value binary, topic string, partition int, offset long"
    )


def fixtures():
    return json.loads(fixture_path("job_posting.v1").read_text(encoding="utf-8"))


def test_columns_match_contract(spark):
    out = bronze_jobs.to_bronze(make_df(spark, kafka_rows(fixtures()))).select(
        *bronze_jobs.BRONZE_COLUMNS
    )
    contract = load_table_contract("bronze.jobs_raw")
    assert [c["name"] for c in contract["columns"]] == bronze_jobs.BRONZE_COLUMNS
    actual = dict(out.dtypes)
    for c in contract["columns"]:
        assert actual[c["name"]] == CONTRACT_TYPES[c["type"]], c["name"]
    nullable = {c["name"] for c in contract["columns"] if c["nullable"]}
    assert set(bronze_jobs.BRONZE_COLUMNS) - set(bronze_jobs.REQUIRED) == nullable


def test_ddl_matches_contract():
    contract = load_table_contract("bronze.jobs_raw")
    for c in contract["columns"]:
        assert f"\n    {c['name']} " in bronze_jobs.DDL, c["name"]
    assert "PARTITIONED BY (days(ingest_ts), source)" in bronze_jobs.DDL
    assert contract["partition_spec"] == ["days(ingest_ts)", "source"]


def test_fixtures_land_unchanged(spark):
    recs = fixtures()
    # So sánh crawl_ts dạng chuỗi UTC trong Spark (Python tự đổi datetime sang múi giờ máy khi collect)
    out = (
        bronze_jobs.to_bronze(make_df(spark, kafka_rows(recs)))
        .withColumn("crawl_ts_utc", F.date_format("crawl_ts", "yyyy-MM-dd'T'HH:mm:ss"))
        .orderBy("kafka_offset")
        .collect()
    )
    assert len(out) == len(recs) and all(r["_valid"] for r in out)
    for row, rec in zip(out, recs, strict=True):
        assert row["source"] == rec["source"]
        assert row["source_job_id"] == rec["source_job_id"]
        assert row["content_hash"] == rec["content_hash"]
        assert row["raw_html_uri"] == rec.get("raw_html_uri")
        assert (
            json.loads(row["payload_json"]) == rec["payload"]
        )  # payload giữ nguyên trạng
        assert row["crawl_ts_utc"] == rec["crawl_ts"][:19]
        assert row["kafka_topic"] == "jobs.raw.topcv.v1"


def test_bad_messages_are_flagged(spark):
    good = fixtures()[0]
    missing = {k: v for k, v in good.items() if k != "content_hash"}
    rows = kafka_rows([good, missing]) + [
        (b"x", b"{not json", "jobs.raw.topcv.v1", 0, 99)
    ]
    out = bronze_jobs.to_bronze(make_df(spark, rows))
    assert out.filter(F.col("_valid")).count() == 1
    assert out.filter(~F.col("_valid")).count() == 2


def test_key_falls_back_to_value(spark):
    rec = fixtures()[0]
    rows = [(None, json.dumps(rec).encode(), "jobs.raw.topcv.v1", 0, 0)]
    out = bronze_jobs.to_bronze(make_df(spark, rows)).collect()[0]
    assert out["source_job_id"] == rec["source_job_id"] and out["_valid"]


def test_topic_pattern():
    import re

    pat = re.compile(bronze_jobs.TOPIC_PATTERN)
    for t in ["jobs.raw.topcv.v1", "jobs.raw.itviec.v1"]:
        assert pat.fullmatch(t)
    for t in [
        "reviews.raw.itviec.v1",
        "dlq.parse_errors.v1",
        "events.user.v1",
        "jobs.raw.topcv.v2",
    ]:
        assert not pat.fullmatch(t)
