"""Kiểm thử lệnh bảo trì Iceberg (spark-jobs/maintenance_iceberg.py)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark-jobs"))
import maintenance_iceberg as M  # noqa: E402

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)


def test_order_and_options():
    sql = M.maintenance_sql("bronze.jobs_raw", NOW)
    assert [s.split("(")[0] for s in sql] == [
        "CALL lake.system.rewrite_data_files",
        "CALL lake.system.rewrite_manifests",
        "CALL lake.system.expire_snapshots",
    ]
    assert "'target-file-size-bytes', '134217728'" in sql[0]
    assert "older_than => TIMESTAMP '2026-10-13 12:00:00'" in sql[2]
    assert "retain_last => 10" in sql[2]  # time travel trong demo vẫn còn


def test_custom_retention():
    sql = M.maintenance_sql("silver.job_postings", NOW, expire_days=1, retain_last=3)
    assert "TIMESTAMP '2026-10-19 12:00:00'" in sql[2] and "retain_last => 3" in sql[2]


@pytest.mark.parametrize("bad", ["jobs_raw", "bronze.jobs_raw'; DROP", "a.b.c", ""])
def test_rejects_bad_table_names(bad):
    with pytest.raises(ValueError):
        M.maintenance_sql(bad, NOW)


def test_default_tables_are_ours():
    assert set(M.DEFAULT_TABLES) == {
        "bronze.jobs_raw",
        "silver.job_postings",
        "silver.job_postings_snapshot",
        "gold.mart_salary_role_region",
        "gold.dim_location",
    }
    mining_tables = {
        "silver.job_skills",
        "silver.job_dedup",
        "silver.review_aspects",
        "silver.reco_scores",
    }
    assert mining_tables.isdisjoint(M.DEFAULT_TABLES)


@pytest.mark.parametrize("table", ["gold.mart_salary_role_region", "gold.dim_location"])
def test_gold_retention_overrides_cli(table):
    assert M.retention_for(table, 7, 10) == (1, 24)
    sql = M.maintenance_sql(table, NOW, *M.retention_for(table, 7, 10))
    assert "older_than => TIMESTAMP '2026-10-19 12:00:00'" in sql[2]
    assert "retain_last => 24" in sql[2]


def test_non_gold_keeps_cli_retention():
    assert M.retention_for("bronze.jobs_raw", 7, 10) == (7, 10)


def test_main_lists_gold_namespace(monkeypatch):
    queries = []

    class FakeSpark:
        def sql(self, query):
            queries.append(query)
            rows = (
                [SimpleNamespace(namespace="gold", tableName="dim_location")]
                if query == "SHOW TABLES IN lake.gold"
                else []
            )
            return SimpleNamespace(collect=lambda: rows)

        def table(self, name):
            return SimpleNamespace(count=lambda: 0)

    monkeypatch.setitem(
        sys.modules,
        "bronze_jobs",
        SimpleNamespace(build_session=lambda name: FakeSpark()),
    )
    M.main(["--tables", "gold.dim_location"])
    assert "SHOW TABLES IN lake.gold" in queries
