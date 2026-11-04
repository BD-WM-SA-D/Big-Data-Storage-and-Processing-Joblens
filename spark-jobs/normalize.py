"""Chuẩn hóa các trường có sẵn của tin tuyển dụng bằng quy tắc (ADR 0001 D2) – không dùng mô hình.

Thuần Python (không phụ thuộc Spark) để kiểm thử nhanh; job silver gọi qua UDF.
Đầu vào là payload job_posting.v1 (TopCV, VietJobs, fixtures). Định dạng đã gặp:
- Lương: "15 - 25 triệu", "15.0 - 22.0 triệu" (VietJobs), "15000000 - 25000000 VND" (TopCV JSON-LD),
  "Từ 12 triệu", "Tới 20 triệu", "Thỏa thuận"/"Thoả thuận"/"Cạnh tranh", "1,000 - 2,000 USD".
- Địa điểm: tên tỉnh có/không dấu, viết thường (VietJobs), tên tỉnh cũ trước 2025, "TP HCM (cũ)".
- Kinh nghiệm: "1 năm", "6 tháng", "Không yêu cầu", "12 tháng" (TopCV monthsOfExperience), "1 - 2 năm".
- Cấp bậc: "Nhân viên", "Trưởng nhóm", "Quản lý / Giám sát", "Thực tập sinh"… (TopCV); VietJobs không có.
"""

from __future__ import annotations

import csv
import html
import os
import re
import unicodedata
from datetime import date
from functools import lru_cache
from pathlib import Path

# Tỷ giá dùng khi lương ghi bằng USD. Giá trị mặc định chỉ để chạy dev – truyền tỷ giá thật qua
# biến môi trường JOBLENS_USD_VND (ghi rõ trong báo cáo ngày lấy tỷ giá).
DEFAULT_USD_VND = 26_000
# Khoảng hợp lệ của lương tháng (VND). Ngoài khoảng này coi như parse sai hoặc ngoại lai và để null.
# 300 triệu/tháng: trên mức này gần như chắc chắn là lỗi nhập (thừa số 0, lương năm ghi nhầm) – thí nghiệm nhiễu
# docs/thi-nghiem-nhieu.md phần 2. Bản trước phần 2 dùng 2 tỷ (BASELINE_SALARY_MAX_VALID, chỉ để đo so sánh).
SALARY_MIN_VALID = 500_000
SALARY_MAX_VALID = 300_000_000
BASELINE_SALARY_MAX_VALID = 2_000_000_000
# Kinh nghiệm yêu cầu trên mức này coi là vô lý (khớp kỳ vọng GX experience_min_years ≤ 40)
EXPERIENCE_MAX_VALID = 40

LEVEL_CODES = ("intern", "junior", "mid", "senior", "lead", "manager", "other")
EMPLOYMENT_CODES = ("full_time", "part_time", "contract", "internship", "other")


# ---------------------------------------------------------------- chuỗi
_TAG = re.compile(r"<[^>]{1,40}>")


def fix_mojibake(text: str) -> str:
    """Sửa lỗi mã hóa "UTF-8 bị đọc thành Latin-1" ("HÃ  Ná»™i" → "Hà Nội").

    Chỉ đổi khi chuỗi mã hóa lại được sang Latin-1 VÀ giải mã được thành UTF-8 khác chuỗi cũ. Tiếng Việt đúng có ký tự
    ngoài Latin-1 (ạ, ộ, ư…) nên không mã hóa được → giữ nguyên; chữ ASCII giải mã ra y hệt → giữ nguyên.
    """
    try:
        fixed = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return fixed


def clean_text(text):
    """Làm sạch chuỗi tự do trước khi chuẩn hóa: sửa mojibake, bỏ thẻ và entity HTML, NFC, gộp khoảng trắng."""
    if text is None:
        return None
    s = fix_mojibake(str(text))
    s = html.unescape(_TAG.sub(" ", s))
    s = unicodedata.normalize("NFC", s).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def fold(text: str | None) -> str:
    """Chuẩn hóa để so khớp: NFC, bỏ dấu, đ→d, chữ thường, chỉ giữ chữ/số, gộp khoảng trắng."""
    if not text:
        return ""
    s = unicodedata.normalize("NFD", str(text))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace("đ", "d").replace("Đ", "D").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


_PLACE_PREFIX = re.compile(r"^(tinh|thanh pho|tp|t p|tx|thi xa)\s+")
_PLACE_SUFFIX = re.compile(r"\s+(cu|moi|city|province)$")


def place_key(text: str | None) -> str:
    s = fold(text)
    s = _PLACE_SUFFIX.sub("", s)
    s = _PLACE_PREFIX.sub("", s)
    return s.replace(" ", "")


# ---------------------------------------------------------------- địa điểm
def _default_locations_csv() -> Path:
    env = os.environ.get("JOBLENS_LOCATIONS_CSV")
    if env:
        return Path(env)
    here = Path(__file__).resolve().parent
    for cand in (
        here / "locations_2025.csv",
        here.parent / "dbt" / "seeds" / "locations_2025.csv",
    ):
        if cand.exists():
            return cand
    raise FileNotFoundError("Không thấy locations_2025.csv – đặt JOBLENS_LOCATIONS_CSV")


@lru_cache(maxsize=4)
def location_index(csv_path: str | None = None) -> dict[str, str]:
    """{place_key: location_code} từ dbt/seeds/locations_2025.csv (tên chính thức + aliases)."""
    path = Path(csv_path) if csv_path else _default_locations_csv()
    index: dict[str, str] = {}
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            for name in [row["name"], *row["aliases"].split("|")]:
                key = place_key(name)
                if key:
                    index.setdefault(key, row["location_code"])
    return index


def osa_distance(a: str, b: str, limit: int = 2) -> int:
    """Khoảng cách chỉnh sửa có tính hoán vị hai ký tự kề nhau (optimal string alignment). Dừng sớm khi > limit."""
    if abs(len(a) - len(b)) > limit:
        return limit + 1
    prev2: list[int] = []
    prev = list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        cur = [i] + [0] * len(b)
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                cur[j] = min(cur[j], prev2[j - 2] + 1)
        if min(cur) > limit:
            return limit + 1
        prev2, prev = prev, cur
    return prev[-1]


@lru_cache(maxsize=8192)
def fuzzy_location(key: str, csv_path: str | None = None) -> str | None:
    """Mã tỉnh cho khóa địa điểm gõ sai một ký tự (lặp, thiếu, thay, đảo). Chỉ khóa ≥ 5 ký tự, và chỉ khi mọi tên ở
    khoảng cách nhỏ nhất cùng một mã tỉnh – không đoán khi mơ hồ."""
    if len(key) < 5:
        return None
    best, codes = 2, set()
    for k, code in location_index(csv_path).items():
        if len(k) < 4:
            continue
        d = osa_distance(key, k, limit=1)
        if d < best:
            best, codes = d, {code}
        elif d == best:
            codes.add(code)
    return codes.pop() if best <= 1 and len(codes) == 1 else None


def location_codes(
    places, csv_path: str | None = None, robust: bool = True
) -> list[str]:
    """Danh sách địa điểm nguyên gốc → mã tỉnh (giữ thứ tự, bỏ trùng). Phần tử có thể chứa nhiều tỉnh nối bằng dấu phẩy.

    robust=False: cách làm trước thí nghiệm nhiễu (không làm sạch chuỗi, không so khớp gần đúng) – chỉ để đo so sánh.
    """
    if places is None:
        return []
    if isinstance(places, str):
        places = [places]
    index = location_index(csv_path)
    out: list[str] = []
    for item in places:
        text = clean_text(item) if robust else item
        for part in re.split(r"[,;|/]", text or ""):
            key = place_key(part) if robust else _baseline_place_key(part)
            code = index.get(key)
            if code is None and robust:
                code = fuzzy_location(key, csv_path)
            if code and code not in out:
                out.append(code)
    return out


def _baseline_place_key(text: str | None) -> str:
    s = fold(text)
    s = re.sub(r"\s+(cu|moi)$", "", s)
    s = _PLACE_PREFIX.sub("", s)
    return s.replace(" ", "")


# ---------------------------------------------------------------- lương
_NUM = re.compile(r"\d[\d.,]*")
_THOUSANDS = re.compile(r"^\d{1,3}(?:[.,]\d{3})+$")


def _to_number(token: str) -> float | None:
    token = token.rstrip(".,")
    if not token:
        return None
    if _THOUSANDS.fullmatch(token):  # 15.000.000 / 1,000
        return float(re.sub(r"[.,]", "", token))
    try:
        return float(token.replace(",", "."))
    except ValueError:
        return None


_YEARLY_RAW = re.compile(r"/\s*(năm|nam|year|yr)\b")
_YEARLY = re.compile(r"\b(moi nam|hang nam|mot nam|per year|per annum|annual|yearly)\b")


def parse_salary(
    text: str | None, usd_vnd: float | None = None, robust: bool = True
) -> tuple[int | None, int | None, bool, str | None]:
    """Chuỗi lương → (min_vnd, max_vnd, is_negotiable, currency_original). Lương tháng; đơn vị giờ/ngày → null;
    lương năm → chia 12 (robust). Giá trị ngoài [SALARY_MIN_VALID, SALARY_MAX_VALID] → null (ngoại lai)."""
    if robust:
        text = clean_text(text)
    if not text or not str(text).strip():
        return None, None, False, None
    raw = str(text)
    f = fold(raw)
    if re.search(r"thoa thuan|canh tranh|negotiable|competitive|thuong luong", f):
        return None, None, True, None
    currency = "USD" if ("usd" in f or "$" in raw) else "VND"
    if re.search(r"/\s*(giờ|gio|ngày|ngay|hour|day|h)\b", raw.lower()) or re.search(
        r"\b(theo gio|moi gio|per hour|per day|theo ngay)\b", f
    ):
        return None, None, False, currency  # không phải lương tháng

    yearly = robust and bool(_YEARLY_RAW.search(raw.lower()) or _YEARLY.search(f))
    nums = [n for n in (_to_number(t) for t in _NUM.findall(raw)) if n is not None]
    if nums and all(n == 0 for n in nums):
        # VietJobs ghi tin "Thỏa thuận" thành "0.0 triệu" (28,5% số tin – khớp con số 71,5% tin có
        # khoảng lương trong bài báo VietJobs) → coi là thỏa thuận, không phải lương 0
        return None, None, True, None
    if not nums:
        return None, None, False, currency

    if currency == "USD":
        rate = usd_vnd or float(os.environ.get("JOBLENS_USD_VND", DEFAULT_USD_VND))
        mult = 1000 if re.search(r"\d\s*k\b", raw.lower()) else 1
        vals = [n * mult * rate for n in nums]
    else:
        # VND: số < 1000 hiểu là triệu ("15 - 25 triệu", "15.0 - 22.0 triệu"); số lớn là VND nguyên
        vals = [n * 1_000_000 if n < 1000 else n for n in nums]
        if robust and re.search(r"\d\s*ty\b", f):  # "1,2 tỷ/năm"
            vals = [n * 1_000_000_000 if n < 1000 else n for n in nums]

    starts_from = re.match(r"^(tu|tren|hon|from|toi thieu|it nhat)\b", f) or re.search(
        r"\b(tro len|upwards)\b", f
    )
    up_to = re.match(r"^(toi|den|duoi|up to|upto|toi da|max)\b", f)
    if yearly:
        vals = [v / 12 for v in vals]
    if len(vals) >= 2:
        lo, hi = sorted(vals[:2])
    elif starts_from:
        lo, hi = vals[0], None
    elif up_to:
        lo, hi = None, vals[0]
    else:
        lo = hi = vals[0]

    upper = SALARY_MAX_VALID if robust else BASELINE_SALARY_MAX_VALID

    def ok(v):
        return (
            int(round(v)) if v is not None and SALARY_MIN_VALID <= v <= upper else None
        )

    lo, hi = ok(lo), ok(hi)
    return lo, hi, False, currency


# ---------------------------------------------------------------- kinh nghiệm
def parse_experience(text, robust: bool = True) -> tuple[float | None, float | None]:
    """Chuỗi kinh nghiệm → (min_năm, max_năm). Một con số trơn ("1 năm") hiểu là yêu cầu đúng mức đó: min = max.
    robust: trên EXPERIENCE_MAX_VALID năm coi là vô lý → (None, None)."""
    lo, hi = _parse_experience(
        clean_text(text) if robust and isinstance(text, str) else text
    )
    if robust and any(v is not None and v > EXPERIENCE_MAX_VALID for v in (lo, hi)):
        return None, None
    return lo, hi


def _parse_experience(text) -> tuple[float | None, float | None]:
    if text is None or (isinstance(text, str) and not text.strip()):
        return None, None
    if isinstance(text, (int, float)):
        return float(text), float(text)
    f = fold(str(text))
    if re.search(
        r"khong yeu cau|chua co kinh nghiem|khong can kinh nghiem|no experience|not required",
        f,
    ):
        return 0.0, None
    nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", str(text).replace(",", "."))]
    if not nums:
        return None, None
    per = 1 / 12 if re.search(r"\bthang\b|\bmonth", f) else 1.0
    vals = [round(n * per, 2) for n in nums]
    if len(vals) >= 2:
        return min(vals[:2]), max(vals[:2])
    v = vals[0]
    if re.search(r"\b(duoi|it hon|under|less than|toi da)\b", f):
        return 0.0, v
    if re.search(
        r"\b(tren|hon|tu|tro len|at least|over|more than|toi thieu)\b", f
    ) or "+" in str(text):
        return v, None
    return v, v


# ---------------------------------------------------------------- cấp bậc
_LEVEL_TEXT = [  # (mẫu trên chuỗi đã fold, mã) – thứ tự quan trọng
    (r"thuc tap|intern|trainee", "intern"),
    (
        r"giam doc|director|truong phong|pho phong|quan ly|giam sat|manager|truong chi nhanh|head of|truong bo phan",
        "manager",
    ),
    (r"truong nhom|team lead|leader|\blead\b|to truong", "lead"),
    (r"senior|chuyen gia|expert|principal", "senior"),
    (r"junior|fresher|moi tot nghiep|entry", "junior"),
    (r"\bmiddle\b|\bmid\b", "mid"),
]
_GENERIC_STAFF = re.compile(
    r"nhan vien|chuyen vien|staff|employee|ky su|engineer|executive|specialist|ky thuat vien"
)
_MANAGER_TITLE = re.compile(
    r"truong phong|pho phong|giam doc|director|manager|head of|truong bo phan|truong chi nhanh|truong ban"
)


def _match_level(f: str) -> str | None:
    for pattern, code in _LEVEL_TEXT:
        if re.search(pattern, f):
            return code
    return None


def level_code(
    level_text: str | None,
    title: str | None = None,
    experience_min: float | None = None,
) -> str | None:
    """Cấp bậc chuẩn. Ưu tiên: chuỗi cấp bậc cụ thể → từ khóa trong tiêu đề → suy từ số năm kinh nghiệm.

    "Nhân viên"/không có cấp bậc: dưới 2 năm → junior, 2 đến dưới 5 năm → mid, từ 5 năm → senior; không biết kinh nghiệm → null.
    """
    lf = fold(level_text)
    code = _match_level(lf)
    if code:
        return code
    ft = fold(title)
    code = _match_level(ft)
    # "Nhân viên giám sát kho", "Chuyên viên quản lý dự án": quản lý/giám sát ở đây là công việc, không phải
    # cấp bậc → chỉ tính manager khi tiêu đề có chức danh quản lý thật hoặc không có từ chỉ nhân viên
    if (
        code == "manager"
        and _GENERIC_STAFF.search(ft)
        and not _MANAGER_TITLE.search(ft)
    ):
        code = None
    if code:
        return code
    if experience_min is not None and (not lf or _GENERIC_STAFF.search(lf)):
        if experience_min < 2:
            return "junior"
        return "mid" if experience_min < 5 else "senior"
    return "other" if lf and not _GENERIC_STAFF.search(lf) else None


# ---------------------------------------------------------------- hình thức làm việc
_EMPLOYMENT = [
    (r"toan thoi gian|full ?time|fulltime", "full_time"),
    (r"ban thoi gian|part ?time|parttime", "part_time"),
    (r"thuc tap|intern", "internship"),
    (
        r"hop dong|thoi vu|contract|temporary|freelance|cong tac vien|tu do|seasonal",
        "contract",
    ),
]


def employment_type_code(text: str | None) -> str | None:
    if not text or not str(text).strip():
        return None
    first = re.split(r"[,;/]", str(text))[0]
    f = fold(first.replace("_", " "))
    for pattern, code in _EMPLOYMENT:
        if re.search(pattern, f):
            return code
    return "other"


# ---------------------------------------------------------------- công ty, ngày
_LEGAL_FORMS = re.compile(
    r"\b(cong ty|cty|co phan|tnhh|trach nhiem huu han|mot thanh vien|hai thanh vien tro len|mtv|jsc|"
    r"joint stock company|company|co ltd|co|ltd|limited|corp|corporation|inc|llc|plc)\b"
)


def company_key(name: str | None) -> str | None:
    """Khóa công ty: bỏ dấu, bỏ loại hình pháp lý (Công ty, TNHH, Cổ phần, JSC, Co., Ltd…), nối bằng '-'."""
    f = fold(name)
    if not f:
        return None
    core = re.sub(r"\s+", " ", _LEGAL_FORMS.sub(" ", f)).strip()
    return (core or f).replace(" ", "-")


def parse_date(text: str | None) -> date | None:
    if not text or not str(text).strip():
        return None
    s = str(text).strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mo, d = map(int, m.groups())
    else:
        m = re.match(r"^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})$", s)
        if not m:
            return None
        d, mo, y = map(int, m.groups())
    try:
        return date(y, mo, d)
    except ValueError:
        return None


# ---------------------------------------------------------------- gộp
def normalize_payload(
    payload: dict | None,
    usd_vnd: float | None = None,
    locations_csv: str | None = None,
    robust: bool = True,
) -> dict:
    """payload job_posting.v1 → các cột chuẩn hóa của silver.job_postings (phần lấy từ payload).

    robust=True (mặc định, pipeline thật): làm sạch chuỗi, lương năm, so khớp tỉnh gần đúng, loại ngoại lai.
    robust=False: cách làm trước thí nghiệm nhiễu – chỉ dùng để đo so sánh (docs/thi-nghiem-nhieu.md).
    """
    p = payload or {}
    smin, smax, negotiable, currency = parse_salary(
        p.get("salary_text"), usd_vnd, robust
    )
    emin, emax = parse_experience(p.get("experience_text"), robust)
    title = (
        clean_text(p.get("title")) if robust else (p.get("title") or "").strip()
    ) or ""
    skills = [str(s).strip() for s in (p.get("skill_tags") or []) if str(s).strip()]
    return {
        "title": title,
        "company_name": p.get("company_name"),
        "company_key": company_key(p.get("company_name")),
        "salary_text": p.get("salary_text"),
        "salary_min_vnd": smin,
        "salary_max_vnd": smax,
        "salary_is_negotiable": negotiable,
        "salary_currency_original": currency,
        "location_codes": location_codes(p.get("locations"), locations_csv, robust),
        "experience_min_years": emin,
        "experience_max_years": emax,
        "level_code": level_code(
            clean_text(p.get("level")) if robust else p.get("level"), title, emin
        ),
        "employment_type_code": employment_type_code(
            clean_text(p.get("employment_type")) if robust else p.get("employment_type")
        ),
        "category_source": p.get("category"),
        "deadline": parse_date(p.get("deadline")),
        "posted_date": parse_date(p.get("date_posted")),
        "skill_tags": skills,
        "description": p.get("description"),
        "requirements": p.get("requirements"),
        "benefits": p.get("benefits"),
    }
