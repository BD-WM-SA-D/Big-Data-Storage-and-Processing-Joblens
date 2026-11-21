"""Kiểm thử logic 4 bảng gold đọc kết quả Web Mining (dim_skill, mart_skill_demand_monthly, dim_company,
mart_company_aspect_sentiment) bằng DuckDB – model dịch từ SQL Trino bằng sqlglot như tests/test_dbt_incremental.py.

Dữ liệu nhỏ dựng tay theo đúng cột trong contract; kiểm các quy tắc nghiệp vụ: đếm theo nhóm tin trùng, bỏ
vietjobs/synthetic, mom_change theo tháng liền trước, ghép review với bronze theo review_uid = sha1(source:id).
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")
jinja2 = pytest.importorskip("jinja2")

MODELS = Path(__file__).resolve().parents[1] / "dbt" / "models" / "gold"
ORDER = ["dim_skill", "mart_skill_demand_monthly", "dim_company", "mart_company_aspect_sentiment"]


def uid(source: str, rid: str) -> str:
    return hashlib.sha1(f"{source}:{rid}".encode()).hexdigest()  # = review_uid của absa-scorer (WM)


def render(model: str) -> str:
    sql = jinja2.Template((MODELS / f"{model}.sql").read_text(encoding="utf-8")).render(
        config=lambda **_: "",
        is_incremental=lambda: False,
        source=lambda schema, table: f"{schema}.{table}",
        ref=lambda name: f"gold.{name}",
        this=f"gold.{model}",
    )
    out = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
    # Trino: to_hex(sha1(varbinary)) → hex; DuckDB: sha1(varchar) đã trả hex
    return re.sub(r"LOWER\(HEX\(SHA1\(ENCODE\((.*?)\)\)\)\)", r"SHA1(\1)", out)


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    c.execute("SET lambda_syntax = 'ENABLE_SINGLE_ARROW'")
    c.execute("CREATE SCHEMA silver; CREATE SCHEMA gold; CREATE SCHEMA bronze")
    c.execute("""CREATE TABLE silver.job_postings AS SELECT * FROM (VALUES
        ('j1', 'topcv', 'Công ty A', 'a', DATE '2026-09-10', TIMESTAMPTZ '2026-10-01 00:00:00+00', 'IT'),
        ('j2', 'itviec', 'A JSC', 'a', NULL, TIMESTAMPTZ '2026-09-20 00:00:00+00', 'IT'),
        ('j3', 'topcv', 'B', 'b', DATE '2026-10-05', TIMESTAMPTZ '2026-10-06 00:00:00+00', 'Sales'),
        ('j6', 'topcv', 'B', 'b', DATE '2026-10-07', TIMESTAMPTZ '2026-10-07 00:00:00+00', 'IT'),
        ('j4', 'vietjobs', 'C', 'c', NULL, TIMESTAMPTZ '2026-10-06 00:00:00+00', 'IT'),
        ('j5', 'synthetic', 'A', 'a', NULL, TIMESTAMPTZ '2026-10-06 00:00:00+00', 'IT')
    ) t(job_uid, source, company_name, company_key, posted_date, first_seen_ts, category_source)""")
    # j1 (TopCV) và j2 (ITviec) là cùng một tin – job-dedup gom vào nhóm j2; j3, j6 chưa qua dedup
    c.execute("""CREATE TABLE silver.job_dedup AS SELECT * FROM (VALUES
        ('j1', 'j2', 0.9), ('j2', 'j2', NULL)) t(job_uid, dup_group_id, score)""")
    c.execute("""CREATE TABLE silver.job_skills AS SELECT * FROM (VALUES
        ('j1', 'sk:python', 'Python'), ('j2', 'sk:python', 'python'), ('j2', 'sk:sql', 'SQL'),
        ('j3', 'sk:python', 'Python'), ('j4', 'sk:python', 'Python'), ('j5', 'sk:sql', 'SQL')
    ) t(job_uid, skill_id, surface_form)""")
    c.execute("""CREATE TABLE bronze.reviews_raw AS SELECT * FROM (VALUES
        ('itviec', 'r1', TIMESTAMPTZ '2026-10-01 00:00:00+00', 1,
         '{"company_name": "Công ty A", "aspect_ratings": {"salary_benefits": 4}}'),
        ('itviec', 'r1', TIMESTAMPTZ '2026-10-02 00:00:00+00', 2,
         '{"company_name": "Công ty A", "aspect_ratings": {"salary_benefits": 2, "culture_fun": null}}'),
        ('itviec', 'r2', TIMESTAMPTZ '2026-10-02 00:00:00+00', 3,
         '{"company_name": "Công ty A", "aspect_ratings": {"salary_benefits": 5}}'),
        ('synthetic', 's1', TIMESTAMPTZ '2026-10-02 00:00:00+00', 4, '{"company_name": "X"}')
    ) t(source, source_review_id, crawl_ts, kafka_offset, payload_json)""")
    r1, r2, s1 = uid("itviec", "r1"), uid("itviec", "r2"), uid("synthetic", "s1")
    c.execute(f"""CREATE TABLE silver.review_aspects AS SELECT * FROM (VALUES
        ('{r1}', 'a', 'salary_benefits', 'positive', 'm1'), ('{r1}', 'a', 'salary_benefits', 'negative', 'm1'),
        ('{r2}', 'a', 'salary_benefits', 'positive', 'm1'), ('{r2}', 'a', 'work_life_balance', 'neutral', 'm1'),
        ('{s1}', 'x', 'salary_benefits', 'positive', 'm1')
    ) t(review_uid, company_key, aspect, polarity, model_version)""")  # fmt: skip
    for m in ORDER:
        c.execute(f"CREATE TABLE gold.{m} AS {render(m)}")
    yield c
    c.close()


def q(con, sql):
    return con.execute(sql).fetchall()


def test_dim_skill_label_is_most_frequent_form(con):
    assert q(con, "SELECT skill_id, label, esco_uri, aliases FROM gold.dim_skill ORDER BY 1") == [
        ("sk:python", "Python", None, ["Python", "python"]),
        ("sk:sql", "SQL", None, ["SQL"]),
    ]


def test_skill_demand_counts_dup_groups_and_skips_vietjobs_synthetic(con):
    rows = q(con, """SELECT strftime(month, '%Y-%m'), skill_id, category_source, job_count, share, mom_change
                    FROM gold.mart_skill_demand_monthly ORDER BY 1, 2, 3""")
    got = {(r[0], r[1], r[2]): r[3:] for r in rows}
    # tháng 9: nhóm {j1, j2} (tháng sớm nhất là posted_date 10/09 của j1) – đếm một lần dù có 2 tin
    assert got[("2026-09", "sk:python", "ALL")] == (1, 1.0, None)  # không có tháng 8 → mom null
    assert got[("2026-09", "sk:sql", "IT")] == (1, 1.0, None)
    # tháng 10: j3 (Sales, python), j6 (IT, không kỹ năng); j4 vietjobs và j5 synthetic bị bỏ
    assert got[("2026-10", "sk:python", "ALL")] == (1, 0.5, pytest.approx(-50.0))
    assert got[("2026-10", "sk:python", "Sales")] == (1, 1.0, None)  # tháng 9 không có tin Sales
    assert ("2026-10", "sk:sql", "ALL") not in got  # j5 synthetic có SQL nhưng bị bỏ


def test_dim_company_merges_jobs_and_reviews(con):
    rows = {r[0]: r[1:] for r in q(con, "SELECT * FROM gold.dim_company")}
    assert set(rows) == {"a", "b", "c"}  # 'x' chỉ có review synthetic → bỏ
    name, sources, jobs, reviews = rows["a"]
    assert sources == ["itviec", "topcv"] and jobs == 1 and reviews == 2  # j1+j2 một nhóm; r1, r2
    assert name in ("A JSC", "Công ty A")
    assert rows["b"][2:] == (2, 0)


def test_company_aspect_sentiment_with_latest_star(con):
    rows = {r[2]: r for r in q(con, "SELECT * FROM gold.mart_company_aspect_sentiment WHERE company_key = 'a'")}
    _, _, _, n, pos, neg, neu, star, version = rows["salary_benefits"]
    assert (n, pos, neg, neu, version) == (3, pytest.approx(2 / 3), pytest.approx(1 / 3), 0.0, "m1")
    assert star == pytest.approx(3.5)  # r1 lấy bản crawl mới nhất (2 sao), r2 5 sao
    assert rows["work_life_balance"][7] is None  # ITviec không có sao cho khía cạnh này
    assert q(con, "SELECT count(*) FROM gold.mart_company_aspect_sentiment WHERE company_key = 'x'") == [(0,)]
