"""Kiểm thử phép biến đổi Kafka → bronze.jobs_raw bằng Spark cục bộ (không cần Kafka, Iceberg hay cụm K8s).

Đầu vào là fixtures job_posting.v1 của repo joblens, giả lập đúng các cột mà nguồn Kafka của Spark trả về.
Phần MERGE được kiểm thử ở mức helper SQL và khóa dedupe; không mô phỏng được commit Iceberg trong unit test.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaException
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


def test_bad_messages_go_to_ingest_errors_with_reason(spark):
    good = fixtures()[0]
    missing = {k: v for k, v in good.items() if k not in ("content_hash", "crawl_ts")}
    cut = json.dumps(good)[
        :60
    ].encode()  # JSON bị cắt cụt (thí nghiệm nhiễu: broken_json)
    rows = kafka_rows([good, missing]) + [(b"x", cut, "jobs.raw.topcv.v1", 0, 99)]
    errs = bronze_jobs.errors(bronze_jobs.to_bronze(make_df(spark, rows))).collect()
    assert [c for c in bronze_jobs.ERROR_COLUMNS] == list(errs[0].asDict())
    by_offset = {r["kafka_offset"]: r for r in errs}
    assert (
        by_offset[99]["reason"] == "invalid_json" and by_offset[99]["kafka_key"] == "x"
    )
    assert by_offset[99]["value_prefix"] == cut.decode()
    other = [r for r in errs if r["kafka_offset"] != 99][0]
    assert other["reason"] == "missing:crawl_ts,content_hash"


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


def test_merge_sql_uses_kafka_key():
    """MERGE chỉ chèn bản chưa có khóa Kafka cho cả bảng tốt lẫn bảng lỗi."""
    for table, view in (
        (bronze_jobs.TABLE, "bronze_jobs_good"),
        (bronze_jobs.ERRORS_TABLE, "bronze_jobs_bad"),
    ):
        sql = bronze_jobs.merge_sql(table, view)
        on = (
            "ON t.kafka_topic = s.kafka_topic "
            "AND t.kafka_partition = s.kafka_partition "
            "AND t.kafka_offset = s.kafka_offset"
        )
        assert sql.startswith(f"MERGE INTO {table} t")
        assert f"USING {view} s" in sql
        assert on in sql
        assert sql.endswith("WHEN NOT MATCHED THEN INSERT *")


def test_dedupe_batch_keeps_one_row_per_key(spark):
    """Trùng khóa trong cùng batch chỉ giữ một dòng cho cả bronze và ingest_errors."""
    rec = fixtures()[0]
    duplicate = (
        rec["source_job_id"].encode(),
        json.dumps({**rec, "content_hash": "ban-sau"}).encode(),
        "jobs.raw.topcv.v1",
        0,
        0,
    )
    transformed = bronze_jobs.to_bronze(make_df(spark, kafka_rows([rec]) + [duplicate]))
    good = bronze_jobs.dedupe_batch(
        transformed.filter(F.col("_valid")), bronze_jobs.BRONZE_COLUMNS
    ).collect()
    assert len(good) == 1

    broken = bronze_jobs.to_bronze(
        make_df(
            spark,
            [(b"x", b"{not json", "jobs.raw.topcv.v1", 0, 99)] * 2,
        )
    )
    errs = bronze_jobs.dedupe_batch(
        bronze_jobs.errors(broken), bronze_jobs.ERROR_COLUMNS
    ).collect()
    assert len(errs) == 1


def test_schema_and_source_allow_list(spark):
    """Tin schema v2 hoặc nguồn lạ bị cách ly vào ingest_errors với reason riêng."""
    good = fixtures()[0]
    v2 = {**good, "schema_version": "job_posting.v2"}
    unknown_source = {**good, "source": "linkedin"}
    out = bronze_jobs.to_bronze(make_df(spark, kafka_rows([good, v2, unknown_source])))
    assert out.filter(F.col("_valid")).count() == 1
    reasons = {
        r["kafka_offset"]: r["reason"] for r in bronze_jobs.errors(out).collect()
    }
    assert reasons[1] == "unsupported_schema"
    assert reasons[2] == "unsupported_source:linkedin"


def test_replay_twice_does_not_duplicate(spark):
    """Mô phỏng MERGE WHEN NOT MATCHED: replay cùng batch hai lần vẫn một dòng cho mỗi khóa Kafka."""
    recs = fixtures()
    transformed = bronze_jobs.to_bronze(make_df(spark, kafka_rows(recs)))
    good = bronze_jobs.dedupe_batch(
        transformed.filter(F.col("_valid")), bronze_jobs.BRONZE_COLUMNS
    )
    sql = bronze_jobs.merge_sql(bronze_jobs.TABLE, "bronze_jobs_good")
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    target: set[tuple] = set()
    for _ in range(2):
        keys = [tuple(r[c] for c in bronze_jobs.KEY_COLUMNS) for r in good.collect()]
        assert len(set(keys)) == len(keys)
        for key in keys:
            target.add(key)
        assert len(target) == len(recs)


def test_audit_rows_measure_gap():
    """End offset là offset tiếp theo: nạp tới end-1 thì gap = 0, thiếu bao nhiêu đo bằng bấy nhiêu."""
    assert [
        f.name for f in bronze_jobs.AUDIT_SCHEMA.fields
    ] == bronze_jobs.AUDIT_COLUMNS
    at = datetime(2026, 11, 25, 12, 0, 0)
    ends = {("jobs.raw.topcv.v1", 0): 100, ("jobs.raw.itviec.v1", 1): 0}
    maxes = {("jobs.raw.topcv.v1", 0): 99}
    rows = bronze_jobs.audit_rows(ends, maxes, at)
    assert [r[:5] for r in rows] == [
        ("jobs.raw.itviec.v1", 1, 0, None, 0),
        ("jobs.raw.topcv.v1", 0, 100, 99, 0),
    ]
    assert rows[0][5] == at
    behind = bronze_jobs.audit_rows(
        {("jobs.raw.topcv.v1", 0): 100}, {("jobs.raw.topcv.v1", 0): 96}, at
    )
    assert behind[0][4] == 3


def test_kafka_end_offsets_dung_admin_client(monkeypatch):
    """Helper chỉ lấy topic khớp pattern và dùng OffsetSpec.latest cho từng partition."""

    class FakeTopic:
        def __init__(self, partitions):
            self.partitions = partitions

    class FakeFuture:
        def __init__(self, offset):
            self.offset = offset

        def result(self):
            return SimpleNamespace(offset=self.offset)

    class FakeAdmin:
        def __init__(self, conf):
            assert conf["bootstrap.servers"] == "localhost:9092"

        def list_topics(self, timeout):
            return SimpleNamespace(
                topics={
                    "jobs.raw.vietjobs.v1": FakeTopic([0, 1]),
                    "events.user.v1": FakeTopic([0]),
                }
            )

        def list_offsets(self, requests, request_timeout):
            assert request_timeout == 10.0
            assert all(spec == "latest" for spec in requests.values())
            return {tp: FakeFuture(100 + index) for index, tp in enumerate(requests)}

    class FakeOffsetSpec:
        @staticmethod
        def latest():
            return "latest"

    class FakeTopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

    monkeypatch.setattr("confluent_kafka.TopicPartition", FakeTopicPartition)
    monkeypatch.setattr("confluent_kafka.admin.AdminClient", FakeAdmin)
    monkeypatch.setattr("confluent_kafka.admin.OffsetSpec", FakeOffsetSpec)
    assert bronze_jobs.kafka_end_offsets("localhost:9092") == {
        ("jobs.raw.vietjobs.v1", 0): 100,
        ("jobs.raw.vietjobs.v1", 1): 101,
    }


def test_write_offset_audit_skips_when_admin_fails(spark, monkeypatch, caplog):
    """Lỗi AdminClient hoặc không có partition chỉ ghi cảnh báo, không làm đổ micro-batch."""

    def fail(bootstrap):
        raise KafkaException("broker không phản hồi")

    monkeypatch.setattr(bronze_jobs, "kafka_end_offsets", fail)
    with caplog.at_level(logging.WARNING, logger="bronze_jobs"):
        assert bronze_jobs.write_offset_audit(spark, "localhost:9092") == 0
    assert "Không lấy được end offset" in caplog.text

    monkeypatch.setattr(bronze_jobs, "kafka_end_offsets", lambda bootstrap: {})
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="bronze_jobs"):
        assert bronze_jobs.write_offset_audit(spark, "localhost:9092") == 0
    assert "Không có partition nào" in caplog.text
