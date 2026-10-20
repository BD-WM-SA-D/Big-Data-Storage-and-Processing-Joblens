"""Kiểm thử lệnh bảo trì Iceberg (spark-jobs/maintenance_iceberg.py)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

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
    # repo này chỉ ghi bronze.*, silver.job_postings* (AGENTS.md) – không bảo trì bảng của repo mining
    assert all(
        t.startswith(("bronze.", "silver.job_postings")) for t in M.DEFAULT_TABLES
    )
