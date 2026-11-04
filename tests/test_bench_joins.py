"""Kiểm thử thí nghiệm tối ưu nhiều join (spark-jobs/bench_joins.py) trên dữ liệu nhỏ, Spark cục bộ."""

from __future__ import annotations

import hashlib
from datetime import date

import pytest

import bench_joins as B


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("test-bench-joins")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def uid(sid: str) -> str:
    return hashlib.sha1(f"vietjobs:{sid}".encode()).hexdigest()


@pytest.fixture(scope="module")
def tables(spark):
    from pyspark.sql import functions as F

    silver = spark.createDataFrame(
        [
            (
                uid(str(i)),
                "mid" if i % 2 else "junior",
                ["01"] if i % 3 else ["01", "79"],
                10_000_000,
                20_000_000,
                "vietjobs" if i != 7 else "synthetic",
            )
            for i in range(30)
        ],  # fmt: skip
        "job_uid string, level_code string, location_codes array<string>, salary_min_vnd long, salary_max_vnd long, source string",
    )
    snapshot = spark.createDataFrame(
        [(uid(str(i)), date(2026, 11, d)) for i in range(30) for d in (1, 2)],
        "job_uid string, snapshot_date date",
    )
    bronze = spark.createDataFrame(
        [("vietjobs", str(i), k) for i in range(30) for k in range(3)],
        "source string, source_job_id string, kafka_offset long",
    ).withColumn("job_uid", F.sha1(F.concat_ws(":", "source", "source_job_id")))
    dim = spark.createDataFrame(
        [("01", "Hà Nội", "north"), ("79", "TP.HCM", "south")],
        "location_code string, name string, region string",
    )
    return silver, snapshot, bronze, dim


def test_all_configs_same_result_and_expected_join_types(spark, tables):
    results = [B.run_config(spark, c, tables, repeat=1) for c in B.CONFIGS]
    assert B.same_results(results)
    by = {r["config"]: r for r in results}
    assert (
        by["smj_only"]["broadcast_hash_join"] == 0
        and by["smj_only"]["sort_merge_join"] >= 3
    )
    # bảng tỉnh tạo bằng createDataFrame không có thống kê → bộ lập kế hoạch tĩnh không broadcast; AQE thì có
    assert by["broadcast"]["broadcast_hash_join"] == 0
    assert (
        by["aqe"]["broadcast_hash_join"] >= 1
        and by["aqe_reorder"]["broadcast_hash_join"] >= 1
    )
    assert by["aqe"]["aqe_final"] and by["aqe_reorder"]["aqe_final"]
    assert not by["smj_only"]["aqe_final"]
    md = B.table_md(results)
    assert md.count("\n") == len(results) + 1 and "seconds_median" in md


def test_naive_and_reordered_query_agree(spark, tables):
    a = sorted(
        tuple(r)
        for r in B.query(*tables, reorder=False)
        .select("region", "level_code", "jobs")
        .collect()
    )
    b = sorted(
        tuple(r)
        for r in B.query(*tables, reorder=True)
        .select("region", "level_code", "jobs")
        .collect()
    )
    assert a == b and a  # synthetic bị loại ở cả hai
    assert (
        "south",
        "junior",
        sum(1 for i in range(30) if i % 3 == 0 and i % 2 == 0 and i != 7),
    ) in a


def test_plan_counts_parses_operators():
    plan = (
        "AdaptiveSparkPlan isFinalPlan=true\n+- == Final Plan ==\n+- SortMergeJoin\n   :- Exchange hashpartitioning(x)\n"
        "   +- BroadcastExchange\n+- BroadcastHashJoin\n+- == Initial Plan ==\n+- SortMergeJoin\n+- SortMergeJoin"
    )
    c = B.plan_counts(plan)
    assert c == {
        "broadcast_hash_join": 1,
        "sort_merge_join": 1,
        "shuffle_exchange": 1,
        "aqe_final": True,
    }
