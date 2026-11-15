"""Kiểm thử bronze → silver.job_postings_history (SCD loại 2) bằng Spark cục bộ, không cần Iceberg.

MERGE INTO chỉ chạy được trên Iceberg (cụm). Vì vậy mọi quyết định I/U/D được tính sẵn bằng DataFrame
(merge_changes) và kiểm thử ở đây; MERGE_SQL chỉ áp đúng các nhãn đó.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from joblens_contracts import fixture_path, load_table_contract

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

import bronze_jobs  # noqa: E402
import silver_job_history as H  # noqa: E402

TYPES = {"string": "string", "long": "bigint", "int": "int", "boolean": "boolean", "double": "double",
         "date": "date", "timestamp": "timestamp", "list<string>": "array<string>"}  # fmt: skip
NOW = datetime(2026, 11, 15, 8, 0, 0)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("test-history")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def base():
    return json.loads(fixture_path("job_posting.v1").read_text(encoding="utf-8"))[0]


def crawl(ts: str, salary: str | None = None, title: str | None = None, locations=None):
    m = json.loads(json.dumps(base()))
    m["crawl_ts"] = ts
    if salary is not None:
        m["payload"]["salary_text"] = salary
    if title is not None:
        m["payload"]["title"] = title
    if locations is not None:
        m["payload"]["locations"] = locations
    return m


def bronze_df(spark, records, offsets=None):
    rows = [
        (bytes(r["source_job_id"], "utf-8"), json.dumps(r, ensure_ascii=False).encode(), "jobs.raw.topcv.v1", 0, off)
        for r, off in zip(records, offsets or range(len(records)), strict=True)
    ]  # fmt: skip
    kafka = spark.createDataFrame(rows, "key binary, value binary, topic string, partition int, offset long")
    return bronze_jobs.to_bronze(kafka).filter("_valid").select(*bronze_jobs.BRONZE_COLUMNS)


def history(spark, records, offsets=None):
    out = H.build_history(H.crawl_states(bronze_df(spark, records, offsets)), NOW)
    return sorted(out.collect(), key=lambda r: (r["job_uid"], r["version_no"]))


def test_columns_follow_contract(spark):
    out = H.build_history(H.crawl_states(bronze_df(spark, [crawl("2026-11-01T00:00:00Z")])), NOW)
    assert out.columns == H.HISTORY_COLUMNS
    try:
        contract = load_table_contract("silver.job_postings_history")
    except (FileNotFoundError, KeyError):
        pytest.skip("contract chưa có trong gói joblens_contracts đã cài (PR joblens chưa merge)")
    assert [c["name"] for c in contract["columns"]] == H.HISTORY_COLUMNS
    actual = dict(out.dtypes)
    flat = H.DDL.replace("\n", " ").replace("(", " ").replace(",", " ")
    for c in contract["columns"]:
        assert actual[c["name"]] == TYPES[c["type"]], c["name"]
        assert f" {c['name']} " in flat, c["name"]
    assert "PARTITIONED BY (bucket(8, job_uid))" in H.DDL


def test_unchanged_recrawls_collapse_into_one_version(spark):
    rows = history(spark, [crawl(f"2026-11-0{d}T00:00:00Z") for d in (1, 2, 3)])
    assert len(rows) == 1
    r = rows[0]
    assert (r["version_no"], r["change_type"], r["changed_fields"], r["is_current"]) == (1, "new", [], True)
    assert r["valid_from"] == datetime(2026, 11, 1) and r["last_seen_ts"] == datetime(2026, 11, 3)
    assert r["valid_to"] is None and r["crawl_count"] == 3 and r["updated_ts"] == NOW


def test_salary_change_opens_new_version(spark):
    rows = history(
        spark,
        [
            crawl("2026-11-01T00:00:00Z", salary="15 - 25 triệu"),
            crawl("2026-11-02T00:00:00Z", salary="15 - 25 triệu"),
            crawl("2026-11-05T00:00:00Z", salary="18 - 28 triệu"),
        ],
    )
    v1, v2 = rows
    assert v1["valid_to"] == v2["valid_from"] == datetime(2026, 11, 5)
    assert not v1["is_current"] and v2["is_current"]
    assert v2["change_type"] == "changed" and v2["changed_fields"] == ["salary_min_vnd", "salary_max_vnd"]
    assert (v1["salary_min_vnd"], v2["salary_min_vnd"]) == (15_000_000, 18_000_000)
    assert v1["crawl_count"] == 2 and v2["crawl_count"] == 1
    assert v1["attr_hash"] != v2["attr_hash"]


def test_revert_to_old_state_is_a_new_version(spark):
    rows = history(
        spark,
        [
            crawl("2026-11-01T00:00:00Z", title="Kỹ sư dữ liệu"),
            crawl("2026-11-02T00:00:00Z", title="Kỹ sư dữ liệu (Tuyển gấp)"),
            crawl("2026-11-03T00:00:00Z", title="Kỹ sư dữ liệu"),
        ],
    )
    assert [r["version_no"] for r in rows] == [1, 2, 3]
    assert [r["changed_fields"] for r in rows] == [[], ["title"], ["title"]]
    assert rows[0]["attr_hash"] == rows[2]["attr_hash"]


def test_location_order_is_not_a_change(spark):
    rows = history(
        spark,
        [
            crawl("2026-11-01T00:00:00Z", locations=["Hà Nội", "Hồ Chí Minh"]),
            crawl("2026-11-02T00:00:00Z", locations=["Hồ Chí Minh", "Hà Nội"]),
        ],
    )
    assert len(rows) == 1 and rows[0]["location_codes"] == sorted(rows[0]["location_codes"])


def test_arrival_order_does_not_matter(spark):
    """Bản crawl đến trễ (offset lớn hơn nhưng crawl_ts sớm hơn) cho cùng lịch sử như khi đến đúng thứ tự."""
    recs = [
        crawl("2026-11-01T00:00:00Z", salary="15 - 25 triệu"),
        crawl("2026-11-03T00:00:00Z", salary="18 - 28 triệu"),
        crawl("2026-11-05T00:00:00Z", salary="20 - 30 triệu"),
    ]
    in_order = history(spark, recs, [0, 1, 2])
    late = history(spark, [recs[2], recs[0], recs[1]], [0, 1, 2])
    key = lambda rows: [(r["version_no"], r["valid_from"], r["valid_to"], r["salary_min_vnd"]) for r in rows]  # noqa: E731
    assert key(in_order) == key(late)
    assert [r["salary_min_vnd"] for r in late] == [15_000_000, 18_000_000, 20_000_000]


def test_exact_duplicate_crawl_counted_once(spark):
    c = crawl("2026-11-01T00:00:00Z")
    rows = history(spark, [c, c], [0, 1])
    assert len(rows) == 1 and rows[0]["crawl_count"] == 1


def _ops(spark, new_records, current_records, new_offsets=None, current_offsets=None):
    new = H.build_history(H.crawl_states(bronze_df(spark, new_records, new_offsets)), NOW)
    cur = H.build_history(H.crawl_states(bronze_df(spark, current_records, current_offsets)), datetime(2026, 11, 1))
    out = H.merge_changes(new, cur)
    assert out.columns == [*H.HISTORY_COLUMNS, "_op"]
    return sorted((r["version_no"], r["_op"]) for r in out.collect())


def test_merge_changes_only_emits_real_changes(spark):
    a = crawl("2026-11-01T00:00:00Z", salary="15 - 25 triệu")
    b = crawl("2026-11-05T00:00:00Z", salary="18 - 28 triệu")
    # đã có v1; bản crawl mới đổi lương → v1 được đóng lại (U: valid_to, is_current), v2 thêm mới (I)
    assert _ops(spark, [a, b], [a]) == [(1, "U"), (2, "I")]
    # chạy lại với cùng dữ liệu → không ghi gì (idempotent)
    assert _ops(spark, [a, b], [a, b]) == []


def test_merge_changes_deletes_versions_merged_by_late_crawl(spark):
    """(1) Hiện có 15tr (01/11) → 18tr (05/11); bản crawl 18tr ngày 03/11 đến trễ → v1 đóng sớm hơn, v2 bắt đầu
    sớm hơn: hai dòng U, không thêm phiên bản. (2) Khi lịch sử mới ngắn hơn (bronze không còn bản crawl ở giữa)
    thì các phiên bản thừa bị xóa (D) trong cùng lệnh MERGE."""
    a = crawl("2026-11-01T00:00:00Z", salary="15 - 25 triệu")
    b = crawl("2026-11-05T00:00:00Z", salary="18 - 28 triệu")
    late = crawl("2026-11-03T00:00:00Z", salary="18 - 28 triệu")
    assert _ops(spark, [a, b, late], [a, b]) == [(1, "U"), (2, "U")]
    # v1 15tr, v2 18tr (05/11), v3 15tr (09/11) đã ghi; thật ra bản 05/11 là bản lỗi đã bị gỡ khỏi bronze
    # (ví dụ replay lại sau khi sửa) → lịch sử mới chỉ còn một phiên bản
    c = crawl("2026-11-09T00:00:00Z", salary="15 - 25 triệu")
    assert _ops(spark, [a, c], [a, b, c]) == [(1, "U"), (2, "D"), (3, "D")]


def test_merge_sql_applies_each_op_once():
    sql = " ".join(H.MERGE_SQL.split())
    assert "ON t.job_uid = s.job_uid AND t.version_no = s.version_no" in sql
    assert "WHEN MATCHED AND s._op = 'D' THEN DELETE" in sql
    assert "WHEN MATCHED AND s._op = 'U' THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED AND s._op = 'I' THEN INSERT" in sql
    assert "job_uid = s.job_uid," not in sql  # không cập nhật khóa
