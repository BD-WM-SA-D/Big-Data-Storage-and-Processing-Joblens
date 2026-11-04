"""Kiểm thử phần 1 của thí nghiệm nhiễu: tiêm nhiễu tất định và đáp án (spark-jobs/noise.py, noise_lab.py)."""

from __future__ import annotations

import json
from collections import Counter

import pytest
from joblens_contracts import load_schema

import noise
import noise_lab
import vietjobs

TS = "2026-11-04T01:00:00Z"
SALARIES = [
    "15.0 - 22.0 triệu",
    "7.5 - 10.0 triệu",
    "0.0 triệu",
    "20.0 - 20.0 triệu",
    None,
]
LOCATIONS = ["hà nội", "hồ chí minh", "đà nẵng", "hà nội, hồ chí minh", "bình dương"]
TITLES = [
    "Nhân Viên Kinh Doanh",
    "Kỹ Sư Phần Mềm",
    "R & D Engineer",
    "Kế toán tổng hợp",
    "Lập trình viên Java",
]


def make_messages(n: int) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append(
            {
                "job_title": TITLES[i % 5],
                "location": LOCATIONS[(i // 5) % 5],
                "salary": SALARIES[(i // 25) % 5] or "",
                "experience_required": f"{i % 6} năm",
                "contract_type": "Toàn thời gian",
                "description": "Mô tả",
            }
        )
    return [vietjobs.to_message(r, i, TS) for i, r in enumerate(rows)]


@pytest.fixture(scope="module")
def msgs():
    return make_messages(2000)


def test_deterministic_for_same_seed(msgs):
    a = noise.inject(msgs, 0.3, seed=7)
    b = noise.inject(msgs, 0.3, seed=7)
    c = noise.inject(msgs, 0.3, seed=8)
    assert a == b
    assert [t.source_job_id for t in a[1]] != [t.source_job_id for t in c[1]]


def test_rate_zero_changes_nothing(msgs):
    out, truths = noise.inject(msgs, 0.0, seed=1)
    assert truths == []
    assert [json.loads(e.value) for e in out] == msgs


def test_rate_one_noises_every_message_once(msgs):
    _, truths = noise.inject(msgs[:300], 1.0, seed=1)
    assert len(truths) == 300
    assert len({t.source_job_id for t in truths}) == 300


@pytest.mark.parametrize("rate", [0.1, 0.2, 0.4])
def test_noisy_share_close_to_rate(msgs, rate):
    _, truths = noise.inject(msgs, rate, seed=42)
    assert abs(len(truths) / len(msgs) - rate) < 0.03


def test_every_type_appears_and_has_expected_behaviour(msgs):
    _, truths = noise.inject(msgs, 1.0, seed=3)
    seen = Counter(t.noise_type for t in truths)
    assert set(seen) == set(noise.NOISE_TYPES)
    allowed = {"recover", "flag", "unknown", "dedup", "keep_latest", "quarantine"}
    assert {t.expected for t in truths} <= allowed


def test_field_noise_keeps_message_valid_against_contract(msgs):
    jsonschema = pytest.importorskip("jsonschema")
    validator = jsonschema.Draft202012Validator(load_schema("job_posting.v1"))
    out, truths = noise.inject(msgs[:500], 1.0, seed=5, types=noise.FIELD_NOISE)
    assert len(truths) > 400  # phần lớn tin áp dụng được ít nhất một loại nhiễu trường
    for e in out:
        m = json.loads(e.value)
        assert not list(validator.iter_errors(m))


def test_field_noise_really_changes_the_field(msgs):
    out, truths = noise.inject(msgs[:500], 1.0, seed=9, types=noise.FIELD_NOISE)
    by_id = {json.loads(e.value)["source_job_id"]: json.loads(e.value) for e in out}
    clean = {m["source_job_id"]: m for m in msgs}
    for t in truths:
        assert (
            by_id[t.source_job_id]["payload"] != clean[t.source_job_id]["payload"]
        ), t.noise_type
        # content_hash tính lại theo payload nhiễu, như crawler thật
        assert (
            by_id[t.source_job_id]["content_hash"]
            != clean[t.source_job_id]["content_hash"]
        )


def test_salary_variants_keep_the_same_value():
    p = make_messages(1)[0]["payload"]  # "15.0 - 22.0 triệu"
    texts = {
        noise._salary_format(p, noise.random.Random(s), "0")[0]["salary_text"]
        for s in range(40)
    }
    assert {"15-22tr", "15.000.000 - 22.000.000 VNĐ", "15tr - 22tr"} <= texts
    assert (
        noise._salary_yearly(p, noise.random.Random(0), "0")[0]["salary_text"]
        == "180 - 264 triệu/năm"
    )
    assert (
        noise._salary_swap(p, noise.random.Random(0), "0")[0]["salary_text"]
        == "22 - 15 triệu"
    )
    noisy, truth = noise._salary_outlier(p, noise.random.Random(0), "0")
    assert noisy["salary_text"] == "1500 - 2200 triệu" and truth.expected == "flag"
    assert json.loads(truth.clean_value) == [15_000_000, 22_000_000]


def test_decimal_millions_use_vietnamese_comma():
    assert noise._millions(7_500_000) == "7,5" and noise._millions(15_000_000) == "15"


def test_mojibake_is_reversible_and_different():
    s = "Hà Nội"
    m = noise.mojibake(s)
    assert m != s and m.encode("latin-1").decode("utf-8") == s


def test_location_abbrev_only_for_known_provinces():
    m = make_messages(30)
    hn = m[0]["payload"]  # hà nội → 01
    out = noise._location_abbrev(hn, noise.random.Random(0), "0")
    assert out is not None and out[0]["locations"][0] in noise.ABBREVIATIONS["01"]
    two = m[15]["payload"]  # "hà nội, hồ chí minh" → 2 tỉnh → không áp dụng
    assert noise._location_abbrev(two, noise.random.Random(0), "0") is None


def test_message_level_noise(msgs):
    out, truths = noise.inject(msgs[:400], 1.0, seed=11, types=noise.MESSAGE_NOISE)
    by_type = {t.noise_type: t for t in truths}
    assert set(by_type) == set(noise.MESSAGE_NOISE)
    values = [(e.key, e.value) for e in out]

    dup = by_type["duplicate"].source_job_id
    assert len([v for k, v in values if k == dup]) == 2

    ooo = by_type["out_of_order"].source_job_id
    first, second = [json.loads(v) for k, v in values if k == ooo]
    assert second["crawl_ts"] < first["crawl_ts"]  # bản cũ đến sau
    assert by_type["out_of_order"].expected == "keep_latest"

    broken = [v for k, v in values if k == by_type["broken_json"].source_job_id][0]
    with pytest.raises(json.JSONDecodeError):
        json.loads(broken)

    missing = json.loads(
        [v for k, v in values if k == by_type["missing_required"].source_job_id][0]
    )
    assert "crawl_ts" not in missing


def test_seq_is_emission_order(msgs):
    out, _ = noise.inject(msgs[:200], 0.5, seed=2)
    assert [e.seq for e in out] == list(range(len(out)))


def test_invalid_arguments(msgs):
    with pytest.raises(ValueError):
        noise.inject(msgs[:5], 1.5, seed=1)
    with pytest.raises(ValueError):
        noise.inject(msgs[:5], 0.1, seed=1, types=("no_such_noise",))


def test_build_rows_shapes(msgs):
    msg_rows, truth_rows, summ = noise_lab.build_rows(msgs[:300], 0.2, 42, "r20_s42")
    assert all(r[0] == "r20_s42" and len(r) == 4 for r in msg_rows)
    assert all(r[0] == "r20_s42" and len(r) == 9 for r in truth_rows)
    assert summ["messages"] == 300 and summ["noisy"] == len(truth_rows)
    assert sum(summ["by_type"].values()) == summ["noisy"]
    assert noise_lab.default_run_id(0.2, 42) == "r20_s42"


# ---------------------------------------------------------------- phần 2: xử lý nhiễu
@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("test-noise")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def processed(spark):
    from datetime import datetime, timezone

    clean_msgs = make_messages(600)
    msg_rows, _, _ = noise_lab.build_rows(clean_msgs, 1.0, 13, "t")
    _, truths = noise.inject(clean_msgs, 1.0, 13)
    df = spark.createDataFrame(
        msg_rows, "run_id string, seq long, kafka_key string, value string"
    )
    now = datetime(2026, 11, 4, tzinfo=timezone.utc)
    out = {}
    for mode in noise_lab.MODES:
        p, q = noise_lab.process(df, "t", mode, now)
        out[mode] = ({r["source_job_id"]: r for r in p.collect()}, q.collect())
    return clean_msgs, {t.source_job_id: t for t in truths}, out


def test_process_columns(processed):
    _, _, out = processed
    row = next(iter(out["robust"][0].values()))
    assert list(row.asDict()) == noise_lab.POSTING_COLUMNS


def test_broken_messages_quarantined_only_in_robust(processed):
    _, truths, out = processed
    broken = {s for s, t in truths.items() if t.expected == "quarantine"}
    assert broken and not (broken & set(out["robust"][0]))  # không lọt vào silver
    q = out["robust"][1]
    assert {r["kafka_key"] for r in q} == broken
    reasons = {truths[r["kafka_key"]].noise_type: r["reason"] for r in q}
    assert reasons["broken_json"] == "invalid_json"
    assert (
        reasons["missing_required"].startswith("missing:")
        and "crawl_ts" in reasons["missing_required"]
    )
    assert out["baseline"][1] == []  # pipeline cũ bỏ im lặng


def test_duplicates_and_out_of_order_handled(processed):
    _, truths, out = processed
    for sid, t in truths.items():
        if t.noise_type == "out_of_order":
            assert "(bản cũ)" not in out["robust"][0][sid]["title"]  # giữ bản mới
            assert "(bản cũ)" not in out["baseline"][0][sid]["title"]


def test_robust_recovers_field_noise(processed):
    import normalize as N

    clean_msgs, truths, out = processed
    clean = {m["source_job_id"]: N.normalize_payload(m["payload"]) for m in clean_msgs}
    fixed = {k: 0 for k in noise.FIELD_NOISE}
    total = {k: 0 for k in noise.FIELD_NOISE}
    for sid, t in truths.items():
        if t.level != "field" or t.expected != "recover":
            continue
        r, c = out["robust"][0][sid], clean[sid]
        total[t.noise_type] += 1
        if t.field == "salary":
            ok = (r["salary_min_vnd"], r["salary_max_vnd"]) == (
                c["salary_min_vnd"],
                c["salary_max_vnd"],
            )
        elif t.field == "location":
            ok = list(r["location_codes"]) == c["location_codes"]
        else:
            ok = r["title"] == c["title"]
        fixed[t.noise_type] += ok
    for k in (
        "salary_format",
        "salary_yearly",
        "salary_swap",
        "encoding_mojibake",
        "unicode_nfd",
        "html_entities",
        "whitespace",
        "location_no_accent",
    ):
        if total[k]:
            assert fixed[k] == total[k], k
    if total["location_typo"]:
        assert fixed["location_typo"] / total["location_typo"] >= 0.7


def test_robust_flags_outliers(processed):
    _, truths, out = processed
    for sid, t in truths.items():
        r = out["robust"][0].get(sid)
        if t.noise_type == "salary_outlier":
            assert r["salary_min_vnd"] is None and r["salary_max_vnd"] is None
        if t.noise_type == "experience_outlier":
            assert r["experience_min_years"] is None
