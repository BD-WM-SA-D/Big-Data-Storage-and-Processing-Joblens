"""Kiểm thử chuyển dòng VietJobs → message job_posting.v1 (spark-jobs/vietjobs.py)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from joblens_contracts import job_uid, load_schema

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark-jobs"))
import normalize as N  # noqa: E402
import vietjobs  # noqa: E402

SAMPLE = ROOT / "tests" / "data" / "vietjobs_format_sample.csv"
TS = "2026-10-19T08:00:00Z"


@pytest.fixture(scope="module")
def messages():
    with SAMPLE.open(encoding="utf-8", newline="") as f:
        return [vietjobs.to_message(r, i, TS) for i, r in vietjobs.read_rows(f)]


def test_columns_match_vietjobs_format():
    import csv

    with SAMPLE.open(encoding="utf-8", newline="") as f:
        assert csv.DictReader(f).fieldnames == vietjobs.EXPECTED_COLUMNS


def test_rows_without_title_are_skipped_but_ids_keep_csv_position(messages):
    # 6 dòng, dòng 4 (0-based) thiếu tiêu đề → 5 tin; id vẫn là vị trí dòng trong CSV (khớp repo Web-mining)
    assert [m["source_job_id"] for m in messages] == ["0", "1", "2", "3", "5"]


def test_messages_validate_against_contract(messages):
    jsonschema = pytest.importorskip("jsonschema")
    validator = jsonschema.Draft202012Validator(load_schema("job_posting.v1"))
    for m in messages:
        errors = sorted(validator.iter_errors(m), key=str)
        assert not errors, [e.message for e in errors]


def test_field_mapping(messages):
    m0, m1 = messages[0], messages[1]
    assert m0["source"] == "vietjobs" and m0["url"] is None and m0["crawl_ts"] == TS
    assert m0["payload"]["salary_text"] == "15.0 - 22.0 triệu"
    assert m0["payload"]["description"].count("\n") == 1  # mô tả nhiều dòng giữ nguyên
    assert m0["payload"]["benefits"] == "Thưởng năm; Lương tháng 13"
    assert m1["payload"]["locations"] == ["hà nội", "hồ chí minh"]
    assert (
        m1["payload"]["skill_tags"] == []
    )  # technical_skills là nhãn LLM, không phải tag nhà tuyển dụng
    assert m1["payload"]["level"] is None and m1["payload"]["company_name"] is None


def test_content_hash_is_stable_and_payload_sensitive():
    row = {
        "job_title": "A",
        "location": "hà nội",
        "salary": "10 triệu",
        "description": "x",
    }
    a = vietjobs.to_message(row, 0, TS)["content_hash"]
    assert (
        a == vietjobs.to_message(row, 0, "2030-01-01T00:00:00Z")["content_hash"]
    )  # không phụ thuộc thời điểm nạp
    assert (
        a != vietjobs.to_message({**row, "salary": "11 triệu"}, 0, TS)["content_hash"]
    )


def test_job_uid_matches_web_mining_convention(messages):
    # Web-mining-joblens/ie/vietjobs.py: job_uid("vietjobs", str(chỉ số dòng))
    assert job_uid("vietjobs", messages[0]["source_job_id"]) == job_uid("vietjobs", "0")


def test_vietjobs_normalizes(messages):
    out = {m["source_job_id"]: N.normalize_payload(m["payload"]) for m in messages}
    assert out["0"]["salary_min_vnd"] == 15_000_000 and out["0"]["location_codes"] == [
        "79"
    ]
    assert (
        out["1"]["location_codes"] == ["01", "79"]
        and out["1"]["employment_type_code"] == "full_time"
    )
    assert (
        out["1"]["level_code"] == "mid"
    )  # 2 năm, không có cấp bậc → suy từ kinh nghiệm
    assert out["2"]["location_codes"] == ["79"]  # Bình Dương gộp vào TP.HCM
    assert (
        out["2"]["level_code"] == "intern"
        and out["2"]["salary_min_vnd"] == out["2"]["salary_max_vnd"] == 4_000_000
    )
    assert out["3"]["salary_is_negotiable"] and out["3"]["experience_min_years"] == 0.5
    assert out["5"]["location_codes"] == ["01"] and out["5"]["level_code"] == "senior"


def test_to_list():
    assert vietjobs.to_list("['a', 'b']") == ["a", "b"]
    assert vietjobs.to_list("[]") == []
    assert vietjobs.to_list("a, b") == ["a", "b"]
    assert vietjobs.to_list(None) == []
