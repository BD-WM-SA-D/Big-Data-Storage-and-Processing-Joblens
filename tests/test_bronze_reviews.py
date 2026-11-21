"""Kiểm thử Kafka → bronze.reviews_raw bằng Spark cục bộ (fixtures review.v1 của repo joblens)."""

from __future__ import annotations

import json

import pytest
from joblens_contracts import fixture_path, load_table_contract

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

import bronze_reviews as R  # noqa: E402

TYPES = {"string": "string", "int": "int", "long": "bigint", "timestamp": "timestamp"}


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("test-bronze-reviews")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def df(spark, values):
    rows = [
        (None, v.encode(), "reviews.raw.itviec.v1", 0, i) for i, v in enumerate(values)
    ]
    return spark.createDataFrame(
        rows, "key binary, value binary, topic string, partition int, offset long"
    )


def test_columns_follow_contract(spark):
    msgs = json.loads(fixture_path("review.v1").read_text(encoding="utf-8"))
    out = R.to_bronze(df(spark, [json.dumps(m, ensure_ascii=False) for m in msgs]))
    good = out.filter("_valid").select(*R.REVIEW_COLUMNS)
    contract = load_table_contract("bronze.reviews_raw")
    assert R.REVIEW_COLUMNS == [c["name"] for c in contract["columns"]]
    assert dict(good.dtypes) == {
        c["name"]: TYPES[c["type"]] for c in contract["columns"]
    }
    rows = good.collect()
    assert len(rows) == len(msgs)
    assert (
        json.loads(rows[0]["payload_json"]) == msgs[0]["payload"]
    )  # payload giữ nguyên
    assert (
        rows[0]["source_review_id"] == msgs[0]["source_review_id"]
    )  # thiếu key Kafka → lấy từ message
    flat = R.DDL.replace("\n", " ")
    for c in contract["columns"]:
        assert f" {c['name']} " in flat
    assert "PARTITIONED BY (days(ingest_ts))" in R.DDL


def test_bad_messages_are_quarantined_with_reason(spark):
    m = json.loads(fixture_path("review.v1").read_text(encoding="utf-8"))[0]
    no_company = {k: v for k, v in m.items() if k != "company_id"}
    out = R.to_bronze(df(spark, ["{not json", json.dumps(no_company)])).collect()
    assert [r["_valid"] for r in out] == [False, False]
    assert (
        out[0]["_reason"] == "invalid_json"
        and out[1]["_reason"] == "missing:company_id"
    )


def test_merge_is_insert_only_on_kafka_key():
    sql = " ".join(R.MERGE_SQL.split())
    assert (
        "t.kafka_topic = s.kafka_topic AND t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset"
        in sql
    )  # noqa: E501
    assert "WHEN NOT MATCHED THEN INSERT *" in sql and "WHEN MATCHED" not in sql
