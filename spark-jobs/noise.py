"""Tiêm nhiễu có kiểm soát vào message job_posting.v1 – phần 1 của thí nghiệm nhiễu (docs/thi-nghiem-nhieu.md).

Thuần Python, tất định: cùng `seed` + cùng `source_job_id` → cùng loại nhiễu, cùng giá trị nhiễu. Mỗi tin được chọn
(xác suất `rate`) nhận đúng MỘT loại nhiễu trong các loại áp dụng được cho tin đó. Kèm theo mỗi tin nhiễu là một dòng
**đáp án** (truth): loại nhiễu, trường bị đổi, giá trị sạch, giá trị nhiễu và hành vi mong đợi của pipeline – phần 3
dùng đáp án này để đo pipeline phát hiện/khôi phục được bao nhiêu.

Chỉ đụng các **trường có sẵn** mà repo này chuẩn hóa (ADR 0001 D2: lương, địa điểm, kinh nghiệm, tiêu đề) và lớp
message (trùng, sai thứ tự, hỏng). Không mô phỏng trùng gần giống giữa các trang – đó là dedup của Web Mining (D5).

Hành vi mong đợi (`expected`):
  recover      pipeline phải ra đúng giá trị như khi đọc tin sạch
  flag         giá trị vô lý – không được đưa vào thống kê (null hoặc gắn cờ ngoại lai)
  unknown      thông tin thực sự mất – kết quả phải là null, không được đoán bừa
  dedup        bản trùng y hệt – silver chỉ còn một dòng
  keep_latest  bản cũ đến sau bản mới – silver giữ bản có crawl_ts mới hơn
  quarantine   message hỏng – không vào silver, phải nằm ở bảng cách ly kèm lý do
"""

from __future__ import annotations

import hashlib
import html
import json
import random
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import normalize as N

FIELD_NOISE = (
    "salary_format",  # cùng giá trị, cách viết khác: "15-25tr", "15.000.000 - 25.000.000 VNĐ", "7,5 triệu"
    "salary_yearly",  # lương năm thay lương tháng
    "salary_swap",  # đảo min/max
    "salary_outlier",  # gõ thừa hai số 0: ×100
    "salary_placeholder",  # "N/A", "-", "Liên hệ", chuỗi rỗng
    "location_typo",  # lặp / mất / đảo một ký tự
    "location_abbrev",  # viết tắt không có trong danh mục alias: "SG", "TP.HCM", "ĐN"
    "location_no_accent",  # không dấu, chữ hoa – nhóm đối chứng (pipeline hiện tại đã xử lý)
    "encoding_mojibake",  # UTF-8 bị đọc thành Latin-1: "Hà Nội" → "HÃ  Ná»™i"
    "unicode_nfd",  # tổ hợp dấu tách rời (NFD) thay cho NFC
    "html_entities",  # "&amp;", "&nbsp;", thẻ <b>
    "whitespace",  # tab, xuống dòng, nhiều khoảng trắng
    "experience_outlier",  # "99 năm"
    "missing_fields",  # mất lương và địa điểm
)
MESSAGE_NOISE = (
    "duplicate",  # gửi lại y hệt
    "out_of_order",  # bản cũ hơn (crawl_ts lùi 1 ngày, lương khác) đến sau bản mới
    "broken_json",  # JSON bị cắt cụt
    "missing_required",  # thiếu crawl_ts
)
NOISE_TYPES = FIELD_NOISE + MESSAGE_NOISE

# Viết tắt / cách gọi phổ biến KHÔNG có trong dbt/seeds/locations_2025.csv (aliases) – theo mã tỉnh
ABBREVIATIONS = {
    "01": ["TP.HN", "Hà Nội City", "HaNoi"],
    "79": ["SG", "TP.HCM", "HCMC", "Sài Gòn City"],
    "48": ["ĐN", "Da Nang City"],
    "31": ["HP", "Hai Phong City"],
    "92": ["CT", "Can Tho City"],
}
PLACEHOLDERS = ["N/A", "-", "Liên hệ", "", "null"]


@dataclass
class Emission:
    """Một message gửi đi theo thứ tự `seq` (giống thứ tự đến Kafka)."""

    seq: int
    key: str
    value: str


@dataclass
class Truth:
    source_job_id: str
    noise_type: str
    level: str  # field | message
    field: str
    expected: str
    clean_value: str | None
    noisy_value: str | None
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------- tiện ích
def _rng(seed: int, source_job_id: str) -> random.Random:
    h = hashlib.sha256(f"{seed}:{source_job_id}".encode()).hexdigest()
    return random.Random(int(h[:16], 16))


def _content_hash(payload: dict) -> str:
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _millions(v: int) -> str:
    """15000000 → "15"; 7500000 → "7,5" (dấu phẩy thập phân kiểu Việt)."""
    m = v / 1_000_000
    return str(int(m)) if m == int(m) else f"{m:.1f}".replace(".", ",")


def _dots(v: int) -> str:
    return f"{v:,}".replace(",", ".")


def _clean_salary(p: dict) -> tuple[int | None, int | None]:
    lo, hi, _, cur = N.parse_salary(p.get("salary_text"))
    return (lo, hi) if cur == "VND" else (None, None)


def mojibake(text: str) -> str:
    """Lỗi mã hóa hay gặp: byte UTF-8 bị giải mã như Latin-1 (đảo ngược được bằng chiều ngược lại)."""
    return text.encode("utf-8").decode("latin-1")


def _typo(word: str, rng: random.Random) -> str:
    letters = [i for i, ch in enumerate(word) if ch.isalpha()]
    if len(letters) < 3:
        return word + word[-1]
    i = rng.choice(letters[1:-1])
    kind = rng.choice(["double", "drop", "swap"])
    if kind == "double":
        return word[: i + 1] + word[i] + word[i + 1 :]
    if kind == "drop":
        return word[:i] + word[i + 1 :]
    return word[: i - 1] + word[i] + word[i - 1] + word[i + 1 :]


# ---------------------------------------------------------------- nhiễu theo trường
# Mỗi hàm: (payload sạch, rng) → (payload nhiễu, Truth) hoặc None nếu không áp dụng được cho tin này.
def _salary_format(p, rng, sid):
    lo, hi = _clean_salary(p)
    if lo is None or hi is None:
        return None
    a, b = _millions(lo), _millions(hi)
    if lo == hi:
        noisy = rng.choice([f"{a}tr", f"{_dots(lo)} VNĐ", f"{a} TRIỆU/tháng"])
    else:
        noisy = rng.choice(
            [
                f"{a}-{b}tr",
                f"{_dots(lo)} - {_dots(hi)} VNĐ",
                f"{a} – {b} TRIỆU",
                f"{a}tr - {b}tr",
                f"Lương: {a} đến {b} triệu/tháng",
            ]
        )
    return {**p, "salary_text": noisy}, Truth(
        sid, "salary_format", "field", "salary", "recover", json.dumps([lo, hi]), noisy
    )


def _salary_yearly(p, rng, sid):
    lo, hi = _clean_salary(p)
    if lo is None or hi is None:
        return None
    noisy = f"{_millions(lo * 12)} - {_millions(hi * 12)} triệu/năm"
    return {**p, "salary_text": noisy}, Truth(
        sid, "salary_yearly", "field", "salary", "recover", json.dumps([lo, hi]), noisy
    )


def _salary_swap(p, rng, sid):
    lo, hi = _clean_salary(p)
    if lo is None or hi is None or lo == hi:
        return None
    noisy = f"{_millions(hi)} - {_millions(lo)} triệu"
    return {**p, "salary_text": noisy}, Truth(
        sid, "salary_swap", "field", "salary", "recover", json.dumps([lo, hi]), noisy
    )


def _salary_outlier(p, rng, sid):
    lo, hi = _clean_salary(p)
    if lo is None or hi is None:
        return None
    noisy = f"{_millions(lo * 100)} - {_millions(hi * 100)} triệu"
    return {**p, "salary_text": noisy}, Truth(
        sid, "salary_outlier", "field", "salary", "flag", json.dumps([lo, hi]), noisy
    )


def _salary_placeholder(p, rng, sid):
    if not p.get("salary_text"):
        return None
    noisy = rng.choice(PLACEHOLDERS)
    return {**p, "salary_text": noisy}, Truth(
        sid, "salary_placeholder", "field", "salary", "unknown", p["salary_text"], noisy
    )


def _clean_locations(p) -> list[str]:
    return N.location_codes(p.get("locations"))


def _location_typo(p, rng, sid):
    locs = p.get("locations") or []
    if not locs or not _clean_locations(p):
        return None
    words = locs[0].split()
    j = max(range(len(words)), key=lambda k: len(words[k]))  # sửa từ dài nhất
    words[j] = _typo(words[j], rng)
    noisy = [" ".join(words), *locs[1:]]
    if N.fold(noisy[0]) == N.fold(locs[0]):
        return None
    return {**p, "locations": noisy}, Truth(
        sid,
        "location_typo",
        "field",
        "location",
        "recover",
        json.dumps(_clean_locations(p)),
        json.dumps(noisy, ensure_ascii=False),
    )


def _location_abbrev(p, rng, sid):
    codes = _clean_locations(p)
    if len(codes) != 1 or codes[0] not in ABBREVIATIONS:
        return None
    noisy = [rng.choice(ABBREVIATIONS[codes[0]])]
    return {**p, "locations": noisy}, Truth(
        sid,
        "location_abbrev",
        "field",
        "location",
        "recover",
        json.dumps(codes),
        json.dumps(noisy, ensure_ascii=False),
    )


def _location_no_accent(p, rng, sid):
    locs = p.get("locations") or []
    if not locs or not _clean_locations(p):
        return None
    noisy = [N.fold(x).upper() for x in locs]
    return {**p, "locations": noisy}, Truth(
        sid,
        "location_no_accent",
        "field",
        "location",
        "recover",
        json.dumps(_clean_locations(p)),
        json.dumps(noisy),
    )


def _encoding_mojibake(p, rng, sid):
    locs = p.get("locations") or []
    if not locs or not _clean_locations(p):
        return None
    noisy = {
        **p,
        "title": mojibake(p.get("title") or ""),
        "locations": [mojibake(x) for x in locs],
        "salary_text": mojibake(p["salary_text"])
        if p.get("salary_text")
        else p.get("salary_text"),
    }
    clean = {
        "title": p.get("title"),
        "location_codes": _clean_locations(p),
        "salary": list(_clean_salary(p)),
    }
    return noisy, Truth(
        sid, "encoding_mojibake", "field", "title+location+salary", "recover",
        json.dumps(clean, ensure_ascii=False), json.dumps(noisy["locations"], ensure_ascii=False),
    )  # fmt: skip


def _unicode_nfd(p, rng, sid):
    title = p.get("title") or ""
    if unicodedata.normalize("NFD", title) == title:
        return None
    noisy = {
        **p,
        "title": unicodedata.normalize("NFD", title),
        "locations": [
            unicodedata.normalize("NFD", x) for x in p.get("locations") or []
        ],
    }
    return noisy, Truth(
        sid, "unicode_nfd", "field", "title", "recover", title, noisy["title"]
    )


def _html_entities(p, rng, sid):
    title = p.get("title") or ""
    if not title:
        return None
    noisy_title = rng.choice(
        [
            f"<b>{html.escape(title)}</b>",
            f"{html.escape(title)}&nbsp;",
            title.replace(" ", "&nbsp;", 1),
        ]
    )
    if " & " in title:
        noisy_title = title.replace(" & ", " &amp; ")
    return {**p, "title": noisy_title}, Truth(
        sid, "html_entities", "field", "title", "recover", title, noisy_title
    )


def _whitespace(p, rng, sid):
    title = p.get("title") or ""
    if " " not in title:
        return None
    noisy = "  " + title.replace(" ", rng.choice(["\t", "  ", " \n "]), 2) + " \t"
    return {**p, "title": noisy}, Truth(
        sid, "whitespace", "field", "title", "recover", title, noisy
    )


def _experience_outlier(p, rng, sid):
    emin, _ = N.parse_experience(p.get("experience_text"))
    if emin is None:
        return None
    noisy = rng.choice(["99 năm", "120 năm", "50 năm"])
    return {**p, "experience_text": noisy}, Truth(
        sid,
        "experience_outlier",
        "field",
        "experience",
        "flag",
        p.get("experience_text"),
        noisy,
    )


def _missing_fields(p, rng, sid):
    if not p.get("salary_text") and not p.get("locations"):
        return None
    noisy = {**p, "salary_text": None, "locations": []}
    clean = {"salary_text": p.get("salary_text"), "locations": p.get("locations")}
    return noisy, Truth(
        sid,
        "missing_fields",
        "field",
        "salary+location",
        "unknown",
        json.dumps(clean, ensure_ascii=False),
        None,
    )


FIELD_FUNCS = {
    "salary_format": _salary_format,
    "salary_yearly": _salary_yearly,
    "salary_swap": _salary_swap,
    "salary_outlier": _salary_outlier,
    "salary_placeholder": _salary_placeholder,
    "location_typo": _location_typo,
    "location_abbrev": _location_abbrev,
    "location_no_accent": _location_no_accent,
    "encoding_mojibake": _encoding_mojibake,
    "unicode_nfd": _unicode_nfd,
    "html_entities": _html_entities,
    "whitespace": _whitespace,
    "experience_outlier": _experience_outlier,
    "missing_fields": _missing_fields,
}


# ---------------------------------------------------------------- tiêm nhiễu cho cả danh sách tin
def _dump(msg: dict) -> str:
    return json.dumps(msg, ensure_ascii=False)


def _with_payload(msg: dict, payload: dict) -> dict:
    return {**msg, "payload": payload, "content_hash": _content_hash(payload)}


def _shift(ts: str, days: int) -> str:
    t = datetime.fromisoformat(ts.replace("Z", "+00:00")) + timedelta(days=days)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def inject(
    messages: list[dict],
    rate: float,
    seed: int,
    types: tuple[str, ...] = NOISE_TYPES,
) -> tuple[list[Emission], list[Truth]]:
    """Danh sách message sạch → (message gửi đi theo thứ tự, đáp án). rate = tỉ lệ tin bị nhiễu (0–1)."""
    if not 0 <= rate <= 1:
        raise ValueError("rate phải trong [0, 1]")
    unknown = set(types) - set(NOISE_TYPES)
    if unknown:
        raise ValueError(f"Loại nhiễu không có: {sorted(unknown)}")
    out: list[Emission] = []
    truths: list[Truth] = []

    def emit(key: str, value: str) -> None:
        out.append(Emission(len(out), key, value))

    for msg in messages:
        sid = msg["source_job_id"]
        rng = _rng(seed, sid)
        if rng.random() >= rate:
            emit(sid, _dump(msg))
            continue
        payload = msg["payload"]
        candidates = []
        for t in types:
            if t in FIELD_FUNCS:
                r = FIELD_FUNCS[t](payload, random.Random(rng.random()), sid)
                if r is not None:
                    candidates.append((t, r))
            else:
                candidates.append((t, None))
        t, r = rng.choice(candidates)
        if r is not None:
            noisy_payload, truth = r
            emit(sid, _dump(_with_payload(msg, noisy_payload)))
            truths.append(truth)
        elif t == "duplicate":
            emit(sid, _dump(msg))
            emit(sid, _dump(msg))
            truths.append(
                Truth(sid, t, "message", "message", "dedup", None, None, {"copies": 2})
            )
        elif t == "out_of_order":
            old_payload = {
                **payload,
                "salary_text": "1 - 2 triệu",
                "title": (payload.get("title") or "") + " (bản cũ)",
            }
            old = {
                **_with_payload(msg, old_payload),
                "crawl_ts": _shift(msg["crawl_ts"], -1),
            }
            emit(sid, _dump(msg))
            emit(sid, _dump(old))  # bản cũ đến SAU bản mới
            truths.append(
                Truth(
                    sid,
                    t,
                    "message",
                    "message",
                    "keep_latest",
                    msg["crawl_ts"],
                    old["crawl_ts"],
                )
            )
        elif t == "broken_json":
            text = _dump(msg)
            emit(sid, text[: max(10, len(text) // 2)])
            truths.append(
                Truth(sid, t, "message", "message", "quarantine", None, "json_cut")
            )
        elif t == "missing_required":
            emit(sid, _dump({k: v for k, v in msg.items() if k != "crawl_ts"}))
            truths.append(
                Truth(sid, t, "message", "message", "quarantine", None, "no_crawl_ts")
            )
    return out, truths


def summary(truths: list[Truth], n_messages: int) -> dict:
    """Số tin bị nhiễu theo loại – in ra log và ghi kèm lượt chạy."""
    by_type: dict[str, int] = {}
    for t in truths:
        by_type[t.noise_type] = by_type.get(t.noise_type, 0) + 1
    return {
        "messages": n_messages,
        "noisy": len(truths),
        "noisy_ratio": round(len(truths) / n_messages, 4) if n_messages else 0.0,
        "by_type": dict(sorted(by_type.items())),
    }
