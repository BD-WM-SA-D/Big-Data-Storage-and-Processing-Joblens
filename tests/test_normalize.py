"""Kiểm thử quy tắc chuẩn hóa (spark-jobs/normalize.py) trên các định dạng thật đã gặp ở TopCV, VietJobs và fixtures."""

from __future__ import annotations

import csv
import json
import sys
from datetime import date
from pathlib import Path

import pytest
from joblens_contracts import fixture_path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark-jobs"))
import normalize as N  # noqa: E402

M = 1_000_000


@pytest.mark.parametrize(
    "text,expected",
    [
        ("15 - 25 triệu", (15 * M, 25 * M, False, "VND")),
        ("15.0 - 22.0 triệu", (15 * M, 22 * M, False, "VND")),  # VietJobs
        ("9.4 - 12.0 triệu", (9_400_000, 12 * M, False, "VND")),
        ("15000000 - 25000000 VND", (15 * M, 25 * M, False, "VND")),  # TopCV JSON-LD
        ("15.000.000 - 20.000.000 đ", (15 * M, 20 * M, False, "VND")),
        ("Từ 12 triệu", (12 * M, None, False, "VND")),
        ("Từ 15000000 VND", (15 * M, None, False, "VND")),
        ("Tới 20 triệu", (None, 20 * M, False, "VND")),
        ("Trên 30 triệu", (30 * M, None, False, "VND")),
        ("10 triệu", (10 * M, 10 * M, False, "VND")),
        ("25 - 15 triệu", (15 * M, 25 * M, False, "VND")),  # đảo thứ tự
        ("Thỏa thuận", (None, None, True, None)),
        ("0.0 triệu", (None, None, True, None)),  # VietJobs ghi tin thỏa thuận thành 0
        ("0 - 0 triệu", (None, None, True, None)),
        ("Thoả thuận", (None, None, True, None)),  # dấu đặt khác chỗ
        ("Cạnh tranh", (None, None, True, None)),
        ("1,000 - 2,000 USD", (26 * M, 52 * M, False, "USD")),
        ("$1500", (39 * M, 39 * M, False, "USD")),
        ("Up to $2k", (None, 52 * M, False, "USD")),
        (
            "25.000 đ/giờ",
            (None, None, False, "VND"),
        ),  # lương giờ – không phải lương tháng
        ("5 triệu/ngày", (None, None, False, "VND")),
        ("", (None, None, False, None)),
        (None, (None, None, False, None)),
        ("liên hệ", (None, None, False, "VND")),
    ],
)
def test_parse_salary(text, expected):
    assert N.parse_salary(text, usd_vnd=26_000) == expected


def test_salary_out_of_range_dropped():
    assert N.parse_salary("50000 - 100000 VND") == (
        None,
        None,
        False,
        "VND",
    )  # quá thấp để là lương tháng


@pytest.mark.parametrize(
    "places,expected",
    [
        (["Hà Nội"], ["01"]),
        (["Hà Nội", "Hồ Chí Minh"], ["01", "79"]),  # TopCV
        ("hà nội, hồ chí minh", ["01", "79"]),  # VietJobs: một chuỗi, nối dấu phẩy
        (["TP HCM (cũ)"], ["79"]),  # fixtures
        (["Thành phố Hồ Chí Minh"], ["79"]),
        (["Ha Noi"], ["01"]),  # không dấu
        (["Bình Dương"], ["79"]),  # tỉnh cũ đã gộp vào TP.HCM
        (["Bắc Giang"], ["24"]),  # mã Bắc Ninh mới là 24
        (["Quảng Bình", "Quảng Trị"], ["44"]),  # cùng một tỉnh mới → không lặp
        (["Tiền Giang"], ["82"]),
        (["Thừa Thiên Huế"], ["46"]),
        (["hà đông"], ["01"]),  # quận của Hà Nội, top 3 VietJobs
        (["Đồng Nai", "Hồ Chí Minh"], ["75", "79"]),
        (["Nước ngoài"], []),
        ([], []),
        (None, []),
    ],
)
def test_location_codes(places, expected):
    assert N.location_codes(places) == expected


def test_location_table_is_complete():
    rows = list(
        csv.DictReader(
            (ROOT / "dbt" / "seeds" / "locations_2025.csv").open(encoding="utf-8")
        )
    )
    assert len(rows) == 34
    assert len({r["location_code"] for r in rows}) == 34
    assert {r["region"] for r in rows} == {"north", "central", "south"}
    # 63 tỉnh cũ đều phải ánh xạ được (34 tên mới + 29 tỉnh đã gộp, tính cả TP)
    old = [
        "Hà Giang",
        "Yên Bái",
        "Bắc Kạn",
        "Vĩnh Phúc",
        "Hòa Bình",
        "Bắc Giang",
        "Thái Bình",
        "Hải Dương",
        "Hà Nam",
        "Nam Định",
        "Quảng Bình",
        "Quảng Nam",
        "Kon Tum",
        "Bình Định",
        "Ninh Thuận",
        "Đắk Nông",
        "Bình Thuận",
        "Phú Yên",
        "Bà Rịa - Vũng Tàu",
        "Bình Dương",
        "Bình Phước",
        "Long An",
        "Sóc Trăng",
        "Hậu Giang",
        "Bến Tre",
        "Trà Vinh",
        "Tiền Giang",
        "Bạc Liêu",
        "Kiên Giang",
    ]
    assert len(old) == 29
    missing = [p for p in old + [r["name"] for r in rows] if not N.location_codes([p])]
    assert missing == []
    # tránh một alias trỏ tới hai tỉnh
    keys = {}
    for r in rows:
        for a in [r["name"], *r["aliases"].split("|")]:
            k = N.place_key(a)
            assert keys.setdefault(k, r["location_code"]) == r["location_code"], a


@pytest.mark.parametrize(
    "text,expected",
    [
        ("1 năm", (1.0, 1.0)),
        ("2 năm", (2.0, 2.0)),
        ("6 tháng", (0.5, 0.5)),
        ("12 tháng", (1.0, 1.0)),  # TopCV monthsOfExperience
        ("Không yêu cầu", (0.0, None)),
        ("Không yêu cầu kinh nghiệm", (0.0, None)),
        ("1 - 2 năm", (1.0, 2.0)),
        ("Dưới 1 năm", (0.0, 1.0)),
        ("Trên 5 năm", (5.0, None)),
        ("3+ năm", (3.0, None)),
        ("1.5 năm", (1.5, 1.5)),
        (2.0, (2.0, 2.0)),
        (None, (None, None)),
        ("", (None, None)),
    ],
)
def test_parse_experience(text, expected):
    assert N.parse_experience(text) == expected


@pytest.mark.parametrize(
    "level,title,exp,expected",
    [
        ("Thực tập sinh", "Thực tập sinh Java", None, "intern"),
        ("Trưởng nhóm", "Team Lead Backend", 5.0, "lead"),
        ("Quản lý / Giám sát", "Giám sát kho", 3.0, "manager"),
        ("Trưởng/Phó phòng", None, None, "manager"),
        (
            "Nhân viên",
            "Senior Java Developer",
            1.0,
            "senior",
        ),  # tiêu đề cụ thể hơn "Nhân viên"
        ("Nhân viên", "Lập trình viên Python", 1.0, "junior"),
        ("Nhân viên", "Kế toán tổng hợp", 3.0, "mid"),
        ("Nhân viên", "Kỹ sư cầu đường", 6.0, "senior"),
        ("Nhân viên", "Nhân viên kinh doanh", None, None),
        (None, "Fresher Data Engineer", None, "junior"),
        (
            None,
            "Nhân Viên Vận Hành",
            0.0,
            "junior",
        ),  # VietJobs: không có cấp bậc → suy từ kinh nghiệm
        (None, "Kỹ thuật viên", None, None),
        ("Phó giám đốc", None, None, "manager"),
        # "giám sát"/"quản lý" là công việc của nhân viên, không phải cấp bậc
        (None, "Nhân Viên Giám Sát Kho", 1.0, "junior"),
        (None, "Chuyên Viên Quản Lý Dự Án", 3.0, "mid"),
        (None, "Kỹ Sư Giám Sát Thi Công", 6.0, "senior"),
        (None, "Quản Lý Cửa Hàng", 2.0, "manager"),
        (None, "Giám Sát Bán Hàng", None, "manager"),
        (None, "Trưởng Phòng Kinh Doanh", 5.0, "manager"),
        (None, "Chuyên Viên Kinh Doanh - Trưởng Phòng", 5.0, "manager"),
    ],
)
def test_level_code(level, title, exp, expected):
    assert N.level_code(level, title, exp) == expected
    assert expected is None or expected in N.LEVEL_CODES


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Toàn thời gian", "full_time"),
        ("FULL_TIME", "full_time"),  # TopCV JSON-LD
        ("Bán thời gian", "part_time"),
        ("PART_TIME", "part_time"),
        ("Thực tập", "internship"),
        ("INTERN", "internship"),
        ("Thời vụ", "contract"),
        ("Cộng tác viên", "contract"),
        (
            "Toàn thời gian, Bán thời gian",
            "full_time",
        ),  # VietJobs ghi nhiều loại – lấy loại đầu
        ("Khác", "other"),
        (None, None),
    ],
)
def test_employment_type(text, expected):
    assert N.employment_type_code(text) == expected


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Công ty Cổ phần FPT Software", "fpt-software"),
        ("CÔNG TY TNHH MTV ABC", "abc"),
        ("FPT Software JSC", "fpt-software"),
        ("ABC Co., Ltd", "abc"),
        ("Công ty Mẫu A", "mau-a"),
        (None, None),
        ("", None),
    ],
)
def test_company_key(name, expected):
    assert N.company_key(name) == expected


def test_parse_date():
    assert N.parse_date("2026-10-31") == date(2026, 10, 31)
    assert N.parse_date("31/10/2026") == date(2026, 10, 31)
    assert N.parse_date("2026-02-30") is None
    assert N.parse_date(None) is None


def test_fixtures_normalize():
    recs = json.loads(fixture_path("job_posting.v1").read_text(encoding="utf-8"))
    out = [N.normalize_payload(r["payload"]) for r in recs]
    assert out[0]["salary_min_vnd"] == 15 * M and out[0]["location_codes"] == ["01"]
    assert out[0]["employment_type_code"] == "full_time" and out[0][
        "posted_date"
    ] == date(2026, 9, 28)
    assert out[1]["salary_is_negotiable"] is True and out[1]["location_codes"] == [
        "79",
        "75",
    ]
    assert out[1]["experience_min_years"] == 0.0
    assert out[2]["salary_min_vnd"] == 12 * M and out[2]["salary_max_vnd"] is None
    assert out[2]["location_codes"] == ["79"]
    for o in out:
        assert isinstance(o["skill_tags"], list) and isinstance(
            o["location_codes"], list
        )
