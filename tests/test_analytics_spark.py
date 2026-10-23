"""Kiểm thử job phân tích Spark (spark-jobs/analytics_spark.py) trên dữ liệu giả lập bằng Spark local:
kế hoạch thực thi đúng thiết kế (broadcast join, sort-merge join), pivot/unpivot khớp tổng, hàm tổng hợp tùy biến,
window functions, thống kê và mô hình MLlib chạy được."""

from __future__ import annotations

import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark-jobs"))
from pyspark.sql import SparkSession  # noqa: E402

import analytics_spark as A  # noqa: E402

M = 1_000_000
T0 = datetime(2026, 10, 20, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s


@pytest.fixture(scope="module")
def data(spark):
    rnd = random.Random(1)
    levels = ["intern", "junior", "mid", "senior", "lead", "manager"]
    base = {
        "intern": 4,
        "junior": 11,
        "mid": 15,
        "senior": 21,
        "lead": 19,
        "manager": 21,
    }
    places = (
        ["01"] * 6 + ["79"] * 5 + ["48", "92", "31"]
    )  # lệch: Hà Nội, TP.HCM chiếm phần lớn
    silver, snap = [], []
    for i in range(400):
        lv = rnd.choice(levels)
        mid = base[lv] * M * rnd.uniform(0.8, 1.2)
        has_salary = rnd.random() < 0.7
        silver.append((
            f"u{i}", "vietjobs" if i % 50 else "synthetic", [rnd.choice(places)],
            int(mid * 0.9) if has_salary else None, int(mid * 1.1) if has_salary else None,
            lv, float(rnd.randint(0, 8)),
        ))  # fmt: skip
        for d in range(rnd.randint(1, 3)):
            snap.append(((T0 + timedelta(days=d)).date(), f"u{i}", d < 2))
    silver_df = spark.createDataFrame(
        silver,
        "job_uid string, source string, location_codes array<string>, salary_min_vnd long, "
        "salary_max_vnd long, level_code string, experience_min_years double",
    )
    snap_df = spark.createDataFrame(
        snap, "snapshot_date date, job_uid string, is_active boolean"
    )
    bronze_df = spark.createDataFrame(
        [(T0 + timedelta(minutes=17 * k), "jobs.raw.vietjobs.v1") for k in range(60)],
        "ingest_ts timestamp, kafka_topic string",
    )
    return silver_df, snap_df, bronze_df


def test_analyze_end_to_end(spark, data):
    silver, snap, bronze = data
    dim = A.dim_locations(spark)
    outputs, summary, plans, cached = A.analyze(
        silver, snap, bronze, dim, T0 - timedelta(days=1)
    )
    try:
        checks = summary["plan_checks"]
        assert checks["broadcast_join"], plans["broadcast_join"]
        assert checks["sort_merge_join"], plans["sort_merge_join"]
        assert checks["pivot_has_levels"]
        assert summary["pivot_total_equals_unpivot_total"]
        assert summary["jobs"] == 392  # bỏ 8 tin synthetic
        assert (
            summary["top_locations_rows"][0]["location_code"] == "01"
        )  # dữ liệu lệch về Hà Nội

        prov = {r["location_code"]: r for r in outputs["salary_by_province"].collect()}
        assert set(prov) <= {"01", "79", "48", "92", "31"}
        assert (
            prov["01"]["rank_in_region"] >= 1
            and 0 < prov["01"]["share_of_postings"] < 1
        )
        assert max(r["cumulative_share"] for r in prov.values()) == pytest.approx(1.0)
        assert prov["01"]["trimmed_mean_salary_vnd"] > 0  # pandas UDF chạy được

        pivot = outputs["level_region_pivot"].collect()
        assert {r["region"] for r in pivot} <= {"north", "central", "south"}
        assert set(A.LEVELS) <= set(pivot[0].asDict())

        hourly = outputs["ingest_hourly"].orderBy("hour").collect()
        assert (
            sum(r["rows"] for r in hourly) == 60
            and hourly[0]["rows_avg_3h"] == hourly[0]["rows"]
        )

        st = summary["statistics"]
        assert -1 <= st["corr_experience_salary"] <= 1
        assert st["salary_quantiles_vnd"]["p10"] <= st["salary_quantiles_vnd"]["p90"]

        m = summary["model"]
        assert m["train_rows"] + m["test_rows"] > 0 and m["grid_size"] == 4
        # cấp bậc quyết định lương trong dữ liệu giả lập → mô hình phải tốt hơn đoán trung vị
        assert m["test_rmse_million_vnd"] < m["baseline_median_rmse_million_vnd"]
    finally:
        for df in cached:
            df.unpersist()


def test_trimmed_mean_drops_outliers():
    import pandas as pd

    v = pd.Series([10.0] * 18 + [1000.0, -1000.0])
    assert A.trimmed_mean.func(v) == 10.0  # bỏ 10% mỗi đầu
    assert pd.isna(A.trimmed_mean.func(pd.Series([], dtype=float)))


def test_plan_checks_detect_pushdown():
    assert A.plan_checks(
        {"partition_pruning": "BatchScan lake.bronze.jobs_raw [filters=ingest_ts >= 1]"}
    )["bronze_filter_pushed"]
    assert not A.plan_checks({"partition_pruning": "Filter (ingest_ts >= 1)"})[
        "bronze_filter_pushed"
    ]
