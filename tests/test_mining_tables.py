"""DDL các bảng silver của Web Mining (spark-jobs/mining_tables.py) phải khớp contract ở repo joblens."""

from __future__ import annotations

import re

import pytest
from joblens_contracts import load_table_contract

import mining_tables as M

SPARK = {"string": "STRING", "int": "INT", "long": "BIGINT", "double": "DOUBLE", "date": "DATE",
         "timestamp": "TIMESTAMP", "boolean": "BOOLEAN"}  # fmt: skip


def columns(name: str) -> list[tuple[str, str, bool]]:
    """(tên, kiểu, nullable) theo thứ tự trong DDL."""
    out = []
    for part in M.TABLES[name][0].split(","):
        m = re.match(r"\s*(\w+)\s+(\w+)(\s+NOT NULL)?\s*$", part)
        assert m, part
        out.append((m.group(1), m.group(2), m.group(3) is None))
    return out


@pytest.mark.parametrize("name", sorted(M.TABLES))
def test_ddl_matches_contract(name):
    c = load_table_contract(name)
    expected = [
        (col["name"], SPARK[col["type"]], col["nullable"]) for col in c["columns"]
    ]
    assert columns(name) == expected
    assert [M.TABLES[name][1]] == c["partition_spec"]
    assert (
        c["writer_repo"] == "Web-mining-joblens"
    )  # bảng của WM: repo này chỉ tạo, không ghi


def test_ddl_is_idempotent_create():
    for name in M.TABLES:
        sql = M.ddl(name)
        assert sql.startswith(f"CREATE TABLE IF NOT EXISTS lake.{name} (")
        assert "USING iceberg" in sql and "'format-version' = '2'" in sql


class FakeCatalog:
    def __init__(self, existing):
        self.existing = set(existing)

    def tableExists(self, name):  # noqa: N802 – cùng tên với API của Spark
        return name in self.existing


class FakeSpark:
    def __init__(self, existing):
        self.catalog = FakeCatalog(existing)
        self.sqls: list[str] = []

    def sql(self, q):
        self.sqls.append(q)


def test_ensure_only_creates_missing_tables():
    s = FakeSpark({"lake.silver.job_skills"})
    assert M.ensure(s) == ["silver.job_dedup", "silver.review_aspects"]
    assert len(s.sqls) == 2 and all("CREATE TABLE IF NOT EXISTS" in q for q in s.sqls)
