"""Kiểm thử logic hai mart gold tăng dần (dbt/models/gold/mart_*_weekly.sql) không cần Trino.

Model được render Jinja (is_incremental bật/tắt), dịch SQL Trino → DuckDB bằng sqlglot rồi chạy trên dữ liệu nhỏ.
Tính chất quan trọng nhất của một model tăng dần: chạy tăng dần sau khi dữ liệu đổi phải cho ĐÚNG kết quả của
dựng lại toàn bộ (--full-refresh). delete+insert theo week được mô phỏng giống dbt-trino.
Cú pháp Trino thật (approx_percentile, transform…) vẫn chỉ được kiểm tra đầy đủ khi `dbt build` trên cụm.
"""

from __future__ import annotations

from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")
jinja2 = pytest.importorskip("jinja2")

MODELS = Path(__file__).resolve().parents[1] / "dbt" / "models" / "gold"


def render(model: str, incremental: bool) -> str:
    sql = jinja2.Template((MODELS / f"{model}.sql").read_text(encoding="utf-8")).render(
        config=lambda **_: "",
        is_incremental=lambda: incremental,
        source=lambda schema, table: f"{schema}.{table}",
        this=f"gold.{model}",
    )
    return sqlglot.transpile(sql, read="trino", write="duckdb")[0]


def build(con, model: str, incremental: bool) -> None:
    """Mô phỏng dbt-trino: lần đầu CREATE TABLE AS; tăng dần: delete các week có trong kết quả mới rồi insert."""
    sql = render(model, incremental)
    if not incremental:
        con.execute(f"CREATE OR REPLACE TABLE gold.{model} AS {sql}")
        return
    con.execute(f"CREATE OR REPLACE TEMP TABLE _new AS {sql}")
    con.execute(f"DELETE FROM gold.{model} WHERE week IN (SELECT week FROM _new)")
    con.execute(f"INSERT INTO gold.{model} SELECT * FROM _new")


def rows(con, model: str):
    cols = "week, source, location_code, level_code" if model == "mart_hiring_weekly" else "week, source, changed_field"
    return con.execute(f"SELECT * EXCLUDE (updated_through) FROM gold.{model} ORDER BY {cols}").fetchall()


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    c.execute("SET lambda_syntax = 'ENABLE_SINGLE_ARROW'")  # sqlglot dịch lambda Trino sang cú pháp x -> …
    c.execute("CREATE SCHEMA silver; CREATE SCHEMA gold")
    yield c
    c.close()


SILVER = """CREATE OR REPLACE TABLE silver.job_postings AS SELECT * FROM (VALUES {rows}) t(job_uid, source, first_seen_ts,
    level_code, location_codes, salary_is_negotiable, salary_min_vnd, salary_max_vnd, updated_ts)"""


def silver_row(uid, week_day, level, locs, neg, lo, hi, updated):
    arr = "[" + ", ".join(f"'{x}'" for x in locs) + "]::VARCHAR[]"
    lo = "NULL" if lo is None else f"{lo}::BIGINT"
    hi = "NULL" if hi is None else f"{hi}::BIGINT"
    return (f"('{uid}', 'topcv', TIMESTAMPTZ '2026-11-{week_day:02d} 10:00:00+00', {level}, {arr}, {neg}, {lo}, {hi}, "
            f"TIMESTAMPTZ '2026-11-{updated:02d} 00:00:00+00')")  # fmt: skip


def test_hiring_weekly_counts_multi_location_jobs_once_in_all(con):
    con.execute(SILVER.format(rows=", ".join([
        silver_row("a", 2, "'junior'", ["01"], "false", 10_000_000, 20_000_000, 10),
        silver_row("b", 3, "'junior'", ["01", "79"], "true", None, None, 10),
        silver_row("c", 4, "NULL", [], "false", 5_000_000, None, 10),
    ])))  # fmt: skip
    build(con, "mart_hiring_weekly", False)
    got = {(r[2], r[3]): r[4:] for r in rows(con, "mart_hiring_weekly")}
    assert got[("ALL", "junior")] == (2, 1, None, 0.5)  # b có 2 tỉnh nhưng chỉ tính 1 lần
    assert got[("01", "junior")][0] == 2 and got[("79", "junior")][0] == 1
    assert got[("unknown", "unknown")][0] == 1 and got[("ALL", "unknown")][0] == 1


def test_hiring_weekly_incremental_equals_full_refresh(con):
    first = [silver_row(f"j{i}", 2 + i % 3, "'mid'", ["01"], "false", 10_000_000 + i, 20_000_000, 5) for i in range(12)]
    first += [silver_row(f"k{i}", 10 + i % 3, "'senior'", ["79"], "false", 30_000_000, 40_000_000, 5) for i in range(6)]
    con.execute(SILVER.format(rows=", ".join(first)))
    build(con, "mart_hiring_weekly", False)
    untouched = [r for r in rows(con, "mart_hiring_weekly") if str(r[0]) == "2026-11-09"]

    # lần chạy sau: 1 tin tuần 02/11 đổi cấp bậc + 1 tin mới tuần 16/11; tuần 09/11 không đổi
    later = [silver_row("j0", 2, "'senior'", ["01"], "false", 10_000_000, 20_000_000, 12)] + first[1:]
    later += [silver_row("n1", 17, "'mid'", ["48"], "true", None, None, 18)]
    con.execute(SILVER.format(rows=", ".join(later)))
    build(con, "mart_hiring_weekly", True)
    inc = rows(con, "mart_hiring_weekly")
    weeks_rebuilt = {str(r[0]) for r in con.execute("SELECT week FROM _new").fetchall()}
    assert weeks_rebuilt == {"2026-11-02", "2026-11-16"}  # chỉ tuần có thay đổi
    build(con, "mart_hiring_weekly", False)
    assert inc == rows(con, "mart_hiring_weekly")
    assert [r for r in inc if str(r[0]) == "2026-11-09"] == untouched


HISTORY = """CREATE OR REPLACE TABLE silver.job_postings_history AS SELECT * FROM (VALUES {rows}) t(job_uid, source,
    version_no, valid_from, changed_fields, salary_min_vnd, salary_max_vnd, updated_ts)"""


def hist_row(uid, v, day, fields, lo, hi, updated):
    arr = "[" + ", ".join(f"'{x}'" for x in fields) + "]::VARCHAR[]"
    lo = "NULL" if lo is None else f"{lo}::BIGINT"
    hi = "NULL" if hi is None else f"{hi}::BIGINT"
    return (f"('{uid}', 'synthetic', {v}, TIMESTAMPTZ '2026-11-{day:02d} 00:00:00+00', {arr}, {lo}, {hi}, "
            f"TIMESTAMPTZ '2026-11-{updated:02d} 00:00:00+00')")  # fmt: skip


def test_job_changes_weekly_salary_direction_and_field_groups(con):
    con.execute(HISTORY.format(rows=", ".join([
        hist_row("a", 1, 2, [], 10_000_000, 20_000_000, 5),
        hist_row("a", 2, 4, ["salary_min_vnd", "salary_max_vnd"], 12_000_000, 24_000_000, 5),  # +20%
        hist_row("b", 1, 2, [], 20_000_000, 20_000_000, 5),
        hist_row("b", 2, 5, ["salary_max_vnd", "title"], 20_000_000, 16_000_000, 5),  # −10%
    ])))  # fmt: skip
    build(con, "mart_job_changes_weekly", False)
    got = {r[2]: r[3:] for r in rows(con, "mart_job_changes_weekly")}
    assert set(got) == {"salary", "title"}  # salary_min/max gộp thành một nhóm, đếm một lần mỗi phiên bản
    change_count, job_count, raises, cuts, pct, days = got["salary"]
    assert (change_count, job_count, raises, cuts) == (2, 2, 1, 1)
    assert pct == pytest.approx(5.0, abs=15)  # trung vị của +20% và −10% (approx_quantile của DuckDB)
    assert got["title"][:4] == (1, 1, None, None)
    assert days == pytest.approx(2.5, abs=0.6)


def test_job_changes_weekly_incremental_equals_full_refresh(con):
    base = [hist_row(f"j{i}", 1, 2, [], 10_000_000, 20_000_000, 5) for i in range(5)]
    base += [hist_row(f"j{i}", 2, 4, ["title"], 10_000_000, 20_000_000, 5) for i in range(3)]
    base += [hist_row("k", 1, 2, [], 10_000_000, 20_000_000, 5), hist_row("k", 2, 10, ["level_code"], 10_000_000, 20_000_000, 5)]  # noqa: E501
    con.execute(HISTORY.format(rows=", ".join(base)))
    build(con, "mart_job_changes_weekly", False)
    # tin j3 có phiên bản 2 mới (lương tăng) ở tuần 16/11, phiên bản cũ ở tuần 02/11 → chỉ tính lại tuần 16/11
    later = base + [hist_row("j3", 2, 17, ["salary_min_vnd"], 11_000_000, 20_000_000, 18)]
    con.execute(HISTORY.format(rows=", ".join(later)))
    build(con, "mart_job_changes_weekly", True)
    assert {str(r[0]) for r in con.execute("SELECT week FROM _new").fetchall()} == {"2026-11-16"}
    inc = rows(con, "mart_job_changes_weekly")
    build(con, "mart_job_changes_weekly", False)
    assert inc == rows(con, "mart_job_changes_weekly")
