"""Bảo trì bảng Iceberg: gộp file nhỏ (compaction), gộp manifest, xóa snapshot cũ.

Vì sao cần: job streaming bronze_jobs commit mỗi micro-batch thành ít nhất một file Parquet cho mỗi phân vùng
(days(ingest_ts), source) – chạy lâu sẽ có rất nhiều file nhỏ, Trino và Spark phải mở từng file nên đọc chậm
(truy vấn 7 trong scripts/demo-queries.sql đo số file). Job này chạy theo lịch trong DAG Airflow joblens_pipeline.

  rewrite_data_files  – gộp file nhỏ thành file ~128 MB (chiến lược binpack). Append của streaming chạy song song
                        không xung đột: Iceberg chỉ thay các file cũ bằng file mới trong một commit.
  rewrite_manifests   – gộp manifest để lập kế hoạch truy vấn nhanh hơn.
  expire_snapshots    – xóa snapshot cũ hơn --expire-days ngày nhưng luôn giữ --retain-last snapshot gần nhất,
                        nên time travel trong buổi demo vẫn còn.

Không chạy remove_orphan_files: khi job streaming đang ghi, file chưa commit có thể bị xóa nhầm.

Chạy:  bash deploy/spark/submit.sh maintenance_iceberg [--tables bronze.jobs_raw] [--expire-days 7]
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

CATALOG = "lake"
DEFAULT_TABLES = [
    "bronze.jobs_raw",
    "silver.job_postings",
    "silver.job_postings_snapshot",
]
TARGET_FILE_BYTES = 128 * 1024 * 1024

log = logging.getLogger("maintenance_iceberg")


def maintenance_sql(
    table: str, now: datetime, expire_days: int = 7, retain_last: int = 10
) -> list[str]:
    """Các lệnh CALL cho một bảng, theo thứ tự chạy. Hàm thuần để kiểm thử không cần Spark."""
    if table.count(".") != 1 or not all(p.isidentifier() for p in table.split(".")):
        raise ValueError(f"Tên bảng phải dạng namespace.table: {table!r}")
    older_than = (now - timedelta(days=expire_days)).strftime("%Y-%m-%d %H:%M:%S")
    return [
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', strategy => 'binpack', "
        f"options => map('min-input-files', '2', 'target-file-size-bytes', '{TARGET_FILE_BYTES}', "
        f"'partial-progress.enabled', 'true'))",
        f"CALL {CATALOG}.system.rewrite_manifests(table => '{table}')",
        f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}', retain_last => {retain_last})",
    ]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description="Compaction và dọn snapshot cho bảng Iceberg"
    )
    p.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
        help="Bảng cần bảo trì, dạng namespace.table",
    )
    p.add_argument("--expire-days", type=int, default=7)
    p.add_argument("--retain-last", type=int, default=10)
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    import bronze_jobs

    spark = bronze_jobs.build_session("maintenance-iceberg")
    now = datetime.now(timezone.utc)
    existing = {
        f"{r.namespace}.{r.tableName}"
        for ns in ("bronze", "silver")
        for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{ns}").collect()
    }
    for table in args.tables:
        if table not in existing:
            log.warning("bỏ qua %s: bảng chưa tồn tại", table)
            continue
        files_before = spark.table(f"{CATALOG}.{table}.files").count()
        for sql in maintenance_sql(table, now, args.expire_days, args.retain_last):
            result = spark.sql(sql).collect()
            log.info("%s → %s", sql.split("(")[0], [r.asDict() for r in result])
        files_after = spark.table(f"{CATALOG}.{table}.files").count()
        log.info("%s: số file dữ liệu %s → %s", table, files_before, files_after)


if __name__ == "__main__":
    main()
