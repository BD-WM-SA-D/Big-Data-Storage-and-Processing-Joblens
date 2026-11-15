"""Kiểm thử bộ mô phỏng crawl lại (spark-jobs/recrawl_sim.py) – hàm thuần, không cần Spark."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime

import normalize
import recrawl_sim as R

END = date(2026, 11, 15)


def jobs(n=200):
    return [
        (str(i), {"title": f"Kỹ sư {i}", "salary_text": "15 - 25 triệu", "employment_type": "Toàn thời gian",
                  "locations": ["Hà Nội"]})
        for i in range(n)
    ]  # fmt: skip


def test_deterministic_and_labelled_synthetic():
    a = R.simulate(jobs(), 28, END, seed=7)
    assert a == R.simulate(jobs(), 28, END, seed=7)
    assert a != R.simulate(jobs(), 28, END, seed=8)
    assert {m["source"] for m in a} == {"synthetic"}
    assert all(m["source_job_id"].startswith("sim-") for m in a)


def test_one_crawl_per_job_per_day_inside_window():
    msgs = R.simulate(jobs(), 28, END)
    per_job = Counter(m["source_job_id"] for m in msgs)
    assert set(per_job) == {f"sim-{i}" for i in range(200)}
    for sid, n in per_job.items():
        days = [m["crawl_ts"][:10] for m in msgs if m["source_job_id"] == sid]
        assert len(set(days)) == n == len(days)  # đúng một lần mỗi ngày
        assert 1 <= n <= 31
    ts = [datetime.strptime(m["crawl_ts"], "%Y-%m-%dT%H:%M:%SZ").date() for m in msgs]
    assert min(ts) >= date(2026, 10, 19) and max(ts) <= END


def test_edits_change_tracked_fields_and_hash():
    msgs = R.simulate(jobs(), 28, END, edit_rate=0.2)
    by_job: dict[str, list[dict]] = {}
    for m in msgs:
        by_job.setdefault(m["source_job_id"], []).append(m)
    raises = cuts = titles = 0
    for seq in by_job.values():
        for prev, cur in zip(seq, seq[1:], strict=False):
            if prev["payload"] == cur["payload"]:
                assert prev["content_hash"] == cur["content_hash"]
                continue
            assert prev["content_hash"] != cur["content_hash"]
            a = normalize.parse_salary(prev["payload"]["salary_text"])[0]
            b = normalize.parse_salary(cur["payload"]["salary_text"])[0]
            raises += b > a
            cuts += b < a
            titles += prev["payload"]["title"] != cur["payload"]["title"]
    assert raises > cuts > 0 and titles > 0  # tăng lương nhiều hơn giảm, có sửa tiêu đề


def test_salary_text_parses_back_with_real_rules():
    assert normalize.parse_salary(R._salary_text(17_250_000, 28_750_000))[:2] == (17_000_000, 29_000_000)
    assert normalize.parse_salary(R._salary_text(20_000_000, None))[:2] == (20_000_000, None)
