"""Kiểm thử cổng chất lượng GX trên silver (spark-jobs/gx_silver.py) bằng Spark local."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("great_expectations")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark-jobs"))
from pyspark.sql import SparkSession  # noqa: E402

import gx_silver as G  # noqa: E402

COLS = (
    "job_uid string, title string, source string, is_active boolean, level_code string, "
    "employment_type_code string, salary_min_vnd long, salary_max_vnd long, "
    "experience_min_years double, location_codes array<string>, last_seen_ts timestamp, "
    "updated_ts timestamp"
)
M = 1_000_000
NOW = datetime(2026, 10, 19, 12, 0)
OLD = NOW - timedelta(days=31)
FRESH = NOW - timedelta(minutes=5)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s


def good_rows(n=20):
    return [
        (
            f"u{i}",
            f"Job {i}",
            "vietjobs",
            True,
            "junior",
            "full_time",
            10 * M,
            15 * M,
            1.0,
            ["01"],
            OLD,
            FRESH,
        )
        for i in range(n)
    ]


def by_key(results):
    return {(r["suite"], r["expectation"], str(r["column"])): r for r in results}


def test_location_codes_are_the_34_provinces():
    codes = G.location_codes()
    assert len(codes) == 34 and "01" in codes and "79" in codes


def test_clean_data_passes(spark):
    df = spark.createDataFrame(good_rows(), COLS)
    results = G.validate(df, G.location_codes(), now=NOW)
    assert all(r["success"] for r in results), [r for r in results if not r["success"]]
    fresh = by_key(results)[
        ("silver_freshness", "expect_max_updated_ts_age", "updated_ts")
    ]
    assert fresh["success"] and "now - max(updated_ts)" in fresh["observed"]
    assert (
        len(results)
        == len(
            G.job_expectations(
                __import__("great_expectations.expectations").expectations
            )
        )
        + 2
    )


def test_bad_rows_are_caught(spark):
    rows = good_rows()
    rows[0] = (
        "u1",
        "dup uid",
        "vietjobs",
        True,
        "junior",
        "full_time",
        10 * M,
        15 * M,
        1.0,
        ["01"],
        OLD,
        FRESH,
    )
    rows[2] = (
        "u2",
        "X",
        "crawler-la",
        True,
        "boss",
        "full_time",
        20 * M,
        5 * M,
        1.0,
        ["99"],
        OLD,
        FRESH,
    )
    results = by_key(
        G.validate(spark.createDataFrame(rows, COLS), G.location_codes(), now=NOW)
    )
    fails = {k for k, r in results.items() if not r["success"]}
    assert (
        "silver_job_postings",
        "expect_column_values_to_be_unique",
        "job_uid",
    ) in fails
    assert (
        "silver_job_postings",
        "expect_column_values_to_be_in_set",
        "source",
    ) in fails
    assert (
        "silver_job_postings",
        "expect_column_values_to_be_in_set",
        "level_code",
    ) in fails
    assert (
        "silver_job_postings",
        "expect_column_pair_values_a_to_be_greater_than_b",
        "salary_max_vnd ≥ salary_min_vnd",
    ) in fails
    assert (
        "silver_location_codes",
        "expect_column_values_to_be_in_set",
        "location_code",
    ) in fails


def test_coverage_uses_mostly_threshold(spark):
    rows = good_rows(20)
    # 1/20 = 5% không có địa điểm → vẫn đạt ngưỡng 90%; 3/20 = 15% → hỏng
    rows[0] = rows[0][:-3] + ([], OLD, FRESH)
    ok = by_key(
        G.validate(spark.createDataFrame(rows, COLS), G.location_codes(), now=NOW)
    )
    assert ok[
        ("silver_job_postings", "expect_column_values_to_be_in_set", "has_location")
    ]["success"]
    for i in (1, 2):
        rows[i] = rows[i][:-3] + ([], OLD, FRESH)
    bad = by_key(
        G.validate(spark.createDataFrame(rows, COLS), G.location_codes(), now=NOW)
    )
    assert not bad[
        ("silver_job_postings", "expect_column_values_to_be_in_set", "has_location")
    ]["success"]


def test_summary_table_mentions_failures():
    txt = G.summary_table(
        [
            {
                "suite": "s",
                "expectation": "expect_x",
                "column": "c",
                "mostly": 1.0,
                "success": False,
                "unexpected_percent": 12.5,
                "observed": None,
            },
            {
                "suite": "s",
                "expectation": "expect_y",
                "column": "d",
                "mostly": 0.9,
                "success": True,
                "unexpected_percent": 0.0,
                "observed": None,
            },
        ]
    )
    assert "HỎNG" in txt and "12.50" in txt and "1/2 kỳ vọng đạt" in txt


def test_nulls_in_optional_columns_are_allowed(spark):
    rows = good_rows()
    rows[3] = (
        "u3",
        "No salary",
        "vietjobs",
        True,
        None,
        None,
        None,
        None,
        None,
        ["79"],
        OLD,
        FRESH,
    )
    results = G.validate(spark.createDataFrame(rows, COLS), G.location_codes(), now=NOW)
    assert all(r["success"] for r in results)
    assert os.environ.get("GX_ANALYTICS_ENABLED") == "false"


def test_freshness_fails_when_all_rows_are_old(spark):
    rows = [r[:-1] + (NOW - timedelta(minutes=181),) for r in good_rows()]
    result = by_key(
        G.validate(spark.createDataFrame(rows, COLS), G.location_codes(), now=NOW)
    )[("silver_freshness", "expect_max_updated_ts_age", "updated_ts")]
    assert not result["success"] and result["warn"]
    assert result["error_threshold_minutes"] == 180
    assert "181.0 phút" in result["observed"]


def test_freshness_warn_uses_env_thresholds(spark, monkeypatch):
    monkeypatch.setenv("JOBLENS_FRESHNESS_WARN_MINUTES", "100")
    monkeypatch.setenv("JOBLENS_FRESHNESS_ERROR_MINUTES", "200")
    rows = [r[:-1] + (NOW - timedelta(minutes=150),) for r in good_rows()]
    result = by_key(
        G.validate(spark.createDataFrame(rows, COLS), G.location_codes(), now=NOW)
    )[("silver_freshness", "expect_max_updated_ts_age", "updated_ts")]
    assert result["success"] and result["warn"]
    assert result["warn_threshold_minutes"] == 100
    assert result["error_threshold_minutes"] == 200
