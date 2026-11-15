"""Biến đổi BỐ CỤC VẬT LÝ của bảng Iceberg và đo số file một truy vấn phải đọc (docs/bien-doi-du-lieu.md, T6).

Dữ liệu không đổi (cùng dòng, cùng giá trị) – chỉ đổi cách xếp dòng vào file, để bộ lập kế hoạch bỏ qua được file.
Iceberg lưu min/max của từng cột cho mỗi file trong manifest; truy vấn có điều kiện trên cột đó chỉ mở file mà
khoảng [min, max] chứa giá trị cần tìm (cùng ý với min/max của row group Parquet, nhưng ở mức file, trước khi đọc).

Hai biến đổi:
  1. bronze.jobs_raw – SẮP XẾP lại theo source_job_id (rewrite_data_files strategy 'sort'), giữ nguyên số file
     (target-file-size = kích thước file trung bình hiện tại). Trước: dòng nằm theo thứ tự nạp, mỗi file chứa id
     rải khắp → tra một tin phải mở gần như mọi file. Sau: mỗi file giữ một đoạn id liền nhau → mở ~1 file.
  2. silver.job_postings_history – PARTITION EVOLUTION: thêm months(valid_from) vào partition spec đang là
     bucket(8, job_uid) (chỉ sửa metadata, không khóa bảng, dữ liệu cũ vẫn đọc được), rồi viết lại file cũ theo spec
     mới. Lý do: lúc đầu bảng chỉ được ghi bằng MERGE theo tin (bucket là đủ); khi gold đọc theo tuần (mart tăng dần)
     thì truy vấn theo thời gian cần cắt bớt file → đổi partition mà không phải tạo lại bảng.

Đo trước/sau cho mỗi bảng (đọc bảng metadata `<bảng>.files`, readable_metrics = min/max của cột):
  - số file, tổng MB;
  - số file "ứng viên" cho truy vấn mẫu = file có [min, max] giao với điều kiện – đúng số file Spark/Trino phải mở.
Kết quả: bảng markdown trong log + s3://joblens/reports/layout/<thời điểm>.json. Số byte Trino thật sự đọc:
scripts/bench-layout.sh (EXPLAIN ANALYZE) chạy trước và sau job này.

Chạy (khi DAG tạm dừng – task iceberg_maintenance gộp file theo binpack sẽ trộn lại bố cục của bronze):
  EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh layout_iceberg [--measure-only] [--only bronze|history]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone

CATALOG = "lake"
BRONZE = "bronze.jobs_raw"
HISTORY = "silver.job_postings_history"
RESULT_PREFIX = "s3://joblens/reports/layout"
MIN_TARGET_BYTES = 1 * 1024 * 1024

log = logging.getLogger("layout_iceberg")


def overlaps(lower, upper, lo, hi) -> bool:
    """File có [lower, upper] giao [lo, hi] không. Thiếu thống kê (None) → phải đọc (an toàn như Iceberg)."""
    if lower is None or upper is None:
        return True
    return not (upper < lo or lower > hi)


def file_stats(files: list[dict], lo, hi) -> dict:
    """files: các dòng {lower, upper, bytes} của bảng metadata `.files` → số file, MB, số file ứng viên."""
    cand = [f for f in files if overlaps(f["lower"], f["upper"], lo, hi)]
    mb = lambda fs: round(sum(f["bytes"] for f in fs) / 1024 / 1024, 2)  # noqa: E731
    return {
        "files": len(files),
        "mb": mb(files),
        "candidate_files": len(cand),
        "candidate_mb": mb(cand),
    }


def sort_sql(table: str, column: str, target_bytes: int, where: str) -> str:
    return (
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', strategy => 'sort', "
        f"sort_order => '{column} ASC NULLS LAST', where => \"{where}\", "
        f"options => map('rewrite-all', 'true', 'target-file-size-bytes', '{target_bytes}'))"
    )


def evolve_sql(table: str, field: str) -> list[str]:
    return [
        f"ALTER TABLE {CATALOG}.{table} ADD PARTITION FIELD {field}",
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', options => map('rewrite-all', 'true'))",
    ]


def table_md(rows: list[dict]) -> str:
    cols = ["table", "probe", "stage", "files", "mb", "candidate_files", "candidate_mb"]
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(out)


def _files(spark, table: str, column: str, keep=None) -> list[dict]:
    """Các file dữ liệu (content = 0) của bảng kèm min/max của `column`; keep(partition) lọc theo partition."""
    df = spark.sql(
        f"SELECT readable_metrics.{column}.lower_bound AS lower, readable_metrics.{column}.upper_bound AS upper, "
        f"file_size_in_bytes AS bytes, partition FROM {CATALOG}.{table}.files WHERE content = 0"
    )
    rows = [r.asDict(recursive=True) for r in df.collect()]
    return [r for r in rows if keep is None or keep(r["partition"])]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Sắp xếp / đổi partition bảng Iceberg và đo số file ứng viên")
    p.add_argument("--measure-only", action="store_true", help="Chỉ đo, không đổi bố cục")
    p.add_argument("--only", choices=["bronze", "history"], default=None)
    p.add_argument("--history-field", default="months(valid_from)")
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    import bronze_jobs

    spark = bronze_jobs.build_session("layout-iceberg")
    results: list[dict] = []

    if args.only in (None, "bronze"):
        probe_id = (
            spark.sql(f"""SELECT source_job_id FROM {CATALOG}.{BRONZE} WHERE source = 'vietjobs'
                          ORDER BY sha1(source_job_id) LIMIT 1""").first()[0]
        )  # một tin bất kỳ nhưng cố định (cùng id ở mọi lần chạy)
        vj = lambda part: part["source"] == "vietjobs"  # noqa: E731
        before = _files(spark, BRONZE, "source_job_id", vj)
        st = file_stats(before, probe_id, probe_id)
        results.append({"table": BRONZE, "probe": f"source_job_id = '{probe_id}'", "stage": "trước", **st})
        if not args.measure_only and before:
            target = max(MIN_TARGET_BYTES, int(sum(f["bytes"] for f in before) / len(before)))
            sql = sort_sql(BRONZE, "source_job_id", target, "source = 'vietjobs'")
            log.info("%s", sql)
            spark.sql(sql).show(truncate=False)
            after = _files(spark, BRONZE, "source_job_id", vj)
            results.append({"table": BRONZE, "probe": f"source_job_id = '{probe_id}'", "stage": "sau",
                            **file_stats(after, probe_id, probe_id)})  # fmt: skip

    if args.only in (None, "history") and spark.catalog.tableExists(f"{CATALOG}.{HISTORY}"):
        hi = datetime.now(timezone.utc).replace(tzinfo=None)
        lo = hi - timedelta(days=7)
        probe = "valid_from trong 7 ngày gần nhất"
        before = _files(spark, HISTORY, "valid_from")
        results.append({"table": HISTORY, "probe": probe, "stage": "trước", **file_stats(before, lo, hi)})
        spec = spark.sql(f"SELECT * FROM {CATALOG}.{HISTORY}.partitions LIMIT 0").schema
        has_month = "valid_from_month" in json.dumps(spec.jsonValue())
        if not args.measure_only and not has_month:
            for sql in evolve_sql(HISTORY, args.history_field):
                log.info("%s", sql)
                spark.sql(sql)
            results.append({"table": HISTORY, "probe": probe, "stage": "sau",
                            **file_stats(_files(spark, HISTORY, "valid_from"), lo, hi)})  # fmt: skip
        elif has_month:
            log.info("%s đã có partition months(valid_from) – bỏ qua bước đổi partition", HISTORY)

    log.info("Kết quả bố cục:\n%s", table_md(results))
    if results and not args.no_upload:
        import boto3
        from botocore.config import Config

        now = datetime.now(timezone.utc)
        key = f"reports/layout/{now.strftime('%Y%m%dT%H%M%SZ')}.json"
        boto3.client("s3", endpoint_url=os.environ["JOBLENS_S3_ENDPOINT"],
                     config=Config(s3={"addressing_style": "path"})).put_object(
            Bucket="joblens", Key=key,
            Body=json.dumps({"run_at": now.isoformat(), "results": results}, ensure_ascii=False, indent=2).encode(),
        )  # fmt: skip
        log.info("đã ghi s3://joblens/%s", key)


if __name__ == "__main__":
    main()
