"""Kiểm thử bronze → silver (build_updates) bằng Spark cục bộ, không cần Iceberg hay cụm."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest
from joblens_contracts import fixture_path, job_uid, load_table_contract

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark-jobs"))
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import bronze_jobs  # noqa: E402
import silver_job_postings as S  # noqa: E402

TYPES = {"string": "string", "long": "bigint", "boolean": "boolean", "double": "double", "date": "date",
         "timestamp": "timestamp", "list<string>": "array<string>"}  # fmt: skip
NOW = datetime(2026, 10, 19, 8, 0, 0)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def bronze_df(spark, records, offsets=None):
    rows = [
        (
            bytes(r["source_job_id"], "utf-8"),
            json.dumps(r, ensure_ascii=False).encode(),
            "jobs.raw.topcv.v1",
            0,
            off,
        )
        for r, off in zip(records, offsets or range(len(records)), strict=True)
    ]
    kafka = spark.createDataFrame(
        rows, "key binary, value binary, topic string, partition int, offset long"
    )
    return (
        bronze_jobs.to_bronze(kafka)
        .filter("_valid")
        .select(*bronze_jobs.BRONZE_COLUMNS)
    )


def fixtures():
    return json.loads(fixture_path("job_posting.v1").read_text(encoding="utf-8"))


def test_schema_matches_contract(spark):
    out = S.build_updates(bronze_df(spark, fixtures()), NOW)
    contract = load_table_contract("silver.job_postings")
    assert out.columns == [c["name"] for c in contract["columns"]] == S.SILVER_COLUMNS
    actual = dict(out.dtypes)
    for c in contract["columns"]:
        assert actual[c["name"]] == TYPES[c["type"]], c["name"]
    for c in contract["columns"]:
        assert f" {c['name']} " in S.DDL_SILVER.replace("\n", " ").replace(
            "(", " "
        ).replace(",", " "), c["name"]
    assert "PARTITIONED BY (months(first_seen_ts), bucket(16, job_uid))" in S.DDL_SILVER


def test_snapshot_ddl_matches_contract():
    contract = load_table_contract("silver.job_postings_snapshot")
    flat = S.DDL_SNAPSHOT.replace("\n", " ").replace("(", " ").replace(",", " ")
    for c in contract["columns"]:
        assert f" {c['name']} " in flat, c["name"]
    assert "PARTITIONED BY (days(snapshot_date))" in S.DDL_SNAPSHOT


def test_job_uid_matches_contracts_package(spark):
    out = (
        S.build_updates(bronze_df(spark, fixtures()), NOW)
        .select("source", "source_job_id", "job_uid")
        .collect()
    )
    for r in out:
        assert r["job_uid"] == job_uid(r["source"], r["source_job_id"])


def test_one_row_per_job_latest_crawl_wins(spark):
    recs = fixtures()
    older = json.loads(json.dumps(recs[0]))
    newer = json.loads(json.dumps(recs[0]))
    older["crawl_ts"] = "2026-09-01T00:00:00Z"
    newer["crawl_ts"] = "2026-10-15T00:00:00Z"
    newer["payload"]["salary_text"] = "20 - 30 triệu"
    out = S.build_updates(
        bronze_df(spark, [newer, recs[1], older, recs[0]], [10, 11, 12, 13]), NOW
    )
    assert out.count() == 2
    row = out.filter(F.col("source_job_id") == recs[0]["source_job_id"]).collect()[0]
    assert row["salary_min_vnd"] == 20_000_000  # bản crawl mới nhất
    assert row["first_seen_ts"] == datetime(2026, 9, 1) and row[
        "last_seen_ts"
    ] == datetime(2026, 10, 15)


def test_normalized_values(spark):
    out = {
        r["source_job_id"]: r
        for r in S.build_updates(bronze_df(spark, fixtures()), NOW).collect()
    }
    a, b, c = out["syn-000001"], out["syn-000002"], out["syn-000003"]
    assert (a["salary_min_vnd"], a["salary_max_vnd"], a["location_codes"]) == (
        15_000_000,
        25_000_000,
        ["01"],
    )
    assert a["level_code"] == "junior" and a["employment_type_code"] == "full_time"
    assert str(a["posted_date"]) == "2026-09-28" and str(a["deadline"]) == "2026-10-31"
    assert b["salary_is_negotiable"] and b["location_codes"] == ["79", "75"]
    assert (
        c["salary_min_vnd"] == 12_000_000
        and c["salary_max_vnd"] is None
        and c["location_codes"] == ["79"]
    )
    for r in out.values():
        assert (
            r["is_active"]
            and r["updated_ts"] == NOW
            and r["title"]
            and r["url"] is None
        )


def test_bad_payload_does_not_break_batch(spark):
    recs = fixtures()
    bad = json.loads(json.dumps(recs[0]))
    bad["source_job_id"] = "syn-bad"
    bad["payload"] = {
        "title": "Tin lạ",
        "salary_text": "???",
        "locations": ["Sao Hỏa"],
        "experience_text": "nhiều",
    }
    out = {
        r["source_job_id"]: r
        for r in S.build_updates(bronze_df(spark, recs + [bad]), NOW).collect()
    }
    assert (
        out["syn-bad"]["location_codes"] == []
        and out["syn-bad"]["salary_min_vnd"] is None
    )
    assert out["syn-bad"]["experience_min_years"] is None and len(out) == 4


def test_merge_sql_updates_all_columns():
    for c in S.SILVER_COLUMNS:
        if c not in ("job_uid", "first_seen_ts"):
            assert f"{c} = s.{c}" in S.MERGE_SQL, c
    assert "LEAST(t.first_seen_ts, s.first_seen_ts)" in S.MERGE_SQL
