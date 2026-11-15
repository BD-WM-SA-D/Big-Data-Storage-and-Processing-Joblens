"""Mô phỏng crawl lại hằng ngày để có dữ liệu thay đổi cho silver.job_postings_history (SCD loại 2).

Vì sao cần: VietJobs chỉ nạp một lần (mỗi tin một bản), còn crawler thật (Web-mining) chưa chạy đủ lâu, nên
chưa có tin nào "đổi lương", "đổi tiêu đề". Job này lấy một mẫu tin VietJobs đang có trong bronze, giả lập một
crawler ghé lại mỗi ngày trong --days ngày gần nhất và thỉnh thoảng nhà tuyển dụng sửa tin:
  - tăng lương 5–20% (60% số lần sửa), giảm 5–15% (15%), thêm/bỏ "(Tuyển gấp)" ở tiêu đề (15%),
    đổi hình thức làm việc (10%).
Mỗi tin xuất hiện từ một ngày ngẫu nhiên, sống 7–30 ngày rồi biến mất (không thấy lại → silver đánh dấu hết hạn).

Dữ liệu sinh ra GHI RÕ là mô phỏng: source = 'synthetic', source_job_id = 'sim-<số dòng VietJobs>', kafka_topic =
'simulation.recrawl'. Không dùng làm số liệu thị trường; chỉ để kiểm thử và demo đường đi bronze → silver →
history → gold tăng dần. Cùng --seed thì cùng dữ liệu (tái lập được). Ghi thẳng vào bronze như backfill VietJobs
(đường A), không qua Kafka, để giữ được crawl_ts của các ngày trong quá khứ.

Chạy:  bash deploy/spark/submit.sh recrawl_sim [--jobs 2000] [--days 28] [--seed 42]
       bash deploy/spark/submit.sh recrawl_sim --clean      # xóa mọi dòng mô phỏng khỏi bronze/silver/history
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
from datetime import date, datetime, time, timedelta, timezone

SOURCE = "synthetic"
PREFIX = "sim-"
TOPIC = "simulation.recrawl"
URGENT = " (Tuyển gấp)"
EMPLOYMENT_TYPES = ["Toàn thời gian", "Bán thời gian", "Hợp đồng", "Thực tập"]

log = logging.getLogger("recrawl_sim")


def _salary_text(lo: int | None, hi: int | None) -> str:
    m = lambda v: f"{round(v / 1_000_000):d}"  # noqa: E731
    if lo and hi:
        return f"{m(lo)} - {m(hi)} triệu"
    return f"Từ {m(lo)} triệu" if lo else f"Tới {m(hi)} triệu"


def _edit(payload: dict, rng: random.Random) -> tuple[dict, str]:
    """Một lần nhà tuyển dụng sửa tin → (payload mới, loại sửa)."""
    import normalize

    p = dict(payload)
    lo, hi, _, _ = normalize.parse_salary(p.get("salary_text"))
    r = rng.random()
    if r < 0.75 and (lo or hi):
        f = 1 + rng.uniform(0.05, 0.20) if r < 0.60 else 1 - rng.uniform(0.05, 0.15)
        p["salary_text"] = _salary_text(lo and int(lo * f), hi and int(hi * f))
        return p, "salary_raise" if f > 1 else "salary_cut"
    if r < 0.90:
        t = p.get("title") or ""
        p["title"] = t[: -len(URGENT)] if t.endswith(URGENT) else t + URGENT
        return p, "title"
    p["employment_type"] = rng.choice(
        [e for e in EMPLOYMENT_TYPES if e != p.get("employment_type")]
    )
    return p, "employment_type"


def _content_hash(payload: dict) -> str:
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def simulate(
    jobs: list[tuple[str, dict]],
    days: int,
    end: date,
    seed: int = 42,
    edit_rate: float = 0.05,
) -> list[dict]:
    """(source_job_id gốc, payload) → các message job_posting.v1 mô phỏng, một message cho mỗi tin mỗi ngày còn sống.

    Hàm thuần: cùng đầu vào + seed thì cùng kết quả (mỗi tin một bộ sinh số ngẫu nhiên riêng theo seed:id).
    """
    start = end - timedelta(days=days - 1)
    out = []
    for src_id, payload in jobs:
        rng = random.Random(f"{seed}:{src_id}")
        first = rng.randrange(days)
        last = min(days - 1, first + rng.randint(7, 30))
        hour = rng.randrange(24)
        p = dict(payload)
        for d in range(first, last + 1):
            if d > first and rng.random() < edit_rate:
                p, _ = _edit(p, rng)
            ts = datetime.combine(start + timedelta(days=d), time(hour), timezone.utc)
            sid = f"{PREFIX}{src_id}"
            out.append(
                {
                    "schema_version": "job_posting.v1",
                    "source": SOURCE,
                    "source_job_id": sid,
                    "url": f"https://example.invalid/sim/{sid}",
                    "crawl_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "lastmod": None,
                    "content_hash": _content_hash(p),
                    "raw_html_uri": None,
                    "payload": p,
                }
            )
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Mô phỏng crawl lại hằng ngày (dữ liệu synthetic) vào bronze"
    )
    ap.add_argument(
        "--jobs", type=int, default=2000, help="Số tin VietJobs lấy làm mẫu"
    )
    ap.add_argument(
        "--days", type=int, default=28, help="Số ngày mô phỏng, kết thúc hôm nay (UTC)"
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--edit-rate",
        type=float,
        default=0.05,
        help="Xác suất tin bị sửa ở mỗi lần crawl lại",
    )
    ap.add_argument(
        "--clean", action="store_true", help="Xóa dữ liệu mô phỏng rồi dừng"
    )
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    from pyspark.sql import functions as F

    import bronze_jobs

    spark = bronze_jobs.build_session("recrawl-sim")
    cat = bronze_jobs.CATALOG
    sim = f"source = '{SOURCE}' AND source_job_id LIKE '{PREFIX}%'"
    if args.clean:
        uids = [r[0] for r in spark.table(bronze_jobs.TABLE).filter(F.expr(sim))
                .select(F.sha1(F.concat_ws(":", "source", "source_job_id"))).distinct().collect()]  # fmt: skip
        for t in (
            "silver.job_postings_history",
            "silver.job_postings_snapshot",
            "silver.job_postings",
        ):
            if uids and spark.catalog.tableExists(f"{cat}.{t}"):
                for i in range(
                    0, len(uids), 1000
                ):  # danh sách IN theo lô để câu lệnh không quá dài
                    in_list = ", ".join(f"'{u}'" for u in uids[i : i + 1000])
                    spark.sql(f"DELETE FROM {cat}.{t} WHERE job_uid IN ({in_list})")
        spark.sql(f"DELETE FROM {bronze_jobs.TABLE} WHERE {sim}")
        log.info(
            "đã xóa %s tin mô phỏng khỏi bronze, silver, snapshot, history", len(uids)
        )
        return

    if spark.table(bronze_jobs.TABLE).filter(F.expr(sim)).limit(1).count():
        raise SystemExit(
            "bronze đã có dữ liệu mô phỏng – chạy --clean trước nếu muốn sinh lại"
        )
    picked = (
        spark.table(bronze_jobs.TABLE)
        .filter("source = 'vietjobs'")
        .dropDuplicates(["source_job_id"])
        .withColumn("_k", F.sha1(F.concat(F.lit(f"{args.seed}:"), "source_job_id")))
        .orderBy("_k")
        .limit(args.jobs)
        .select("source_job_id", "payload_json")
        .collect()
    )
    jobs = [(r["source_job_id"], json.loads(r["payload_json"])) for r in picked]
    msgs = simulate(
        jobs, args.days, datetime.now(timezone.utc).date(), args.seed, args.edit_rate
    )
    rows = [(TOPIC, -1, i, m["source_job_id"].encode(), json.dumps(m, ensure_ascii=False).encode())
            for i, m in enumerate(msgs)]  # fmt: skip
    df = spark.createDataFrame(
        rows, "topic string, partition int, offset long, key binary, value binary"
    )
    out = (
        bronze_jobs.to_bronze(df)
        .filter(F.col("_valid"))
        .select(*bronze_jobs.BRONZE_COLUMNS)
    )
    out.writeTo(bronze_jobs.TABLE).append()
    log.info("mô phỏng %s tin × %s ngày → %s lần crawl ghi vào %s (source = %s)",
             len(jobs), args.days, len(msgs), bronze_jobs.TABLE, SOURCE)  # fmt: skip


if __name__ == "__main__":
    main()
