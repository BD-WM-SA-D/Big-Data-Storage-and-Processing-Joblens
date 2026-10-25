"""Kiểm tra câu lệnh mẫu của SQLPad (deploy/data-tools/sqlpad-queries/): đúng định dạng seed và chỉ ghi vào
bảng/collection thử (iceberg.sandbox.*, mongodb.joblens.web_notes) – không ghi bảng của pipeline hay của repo khác."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

QUERIES = sorted(
    (Path(__file__).parents[1] / "deploy/data-tools/sqlpad-queries").glob("*.json")
)
WRITE = re.compile(
    r"^\s*(INSERT\s+INTO|UPDATE|DELETE\s+FROM|DROP\s+TABLE(?:\s+IF\s+EXISTS)?|"
    r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+([\w.\"$]+)",
    re.IGNORECASE | re.MULTILINE,
)
ALLOWED = re.compile(r"^(iceberg\.sandbox\.\w+|mongodb\.joblens\.web_notes)$")


def statements(sql: str) -> list[str]:
    """Bỏ dòng chú thích rồi tách theo ';' (đủ cho câu mẫu – không có ';' trong chuỗi)."""
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    return [s.strip() for s in body.split(";") if s.strip()]


def test_has_queries():
    assert len(QUERIES) >= 5


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.stem)
def test_seed_format(path: Path):
    q = json.loads(path.read_text(encoding="utf-8"))
    assert q["id"] == path.stem
    assert q["connectionId"] == "trino"
    assert q["name"] and q["queryText"].strip()
    assert statements(q["queryText"]), "câu mẫu phải có ít nhất một câu lệnh"


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.stem)
def test_writes_only_to_sandbox(path: Path):
    sql = json.loads(path.read_text(encoding="utf-8"))["queryText"]
    for stmt in statements(sql):
        for _, target in WRITE.findall(stmt):
            assert ALLOWED.match(target), f"{path.name} ghi vào {target}"


def test_ids_unique():
    ids = [json.loads(p.read_text(encoding="utf-8"))["id"] for p in QUERIES]
    assert len(ids) == len(set(ids))
