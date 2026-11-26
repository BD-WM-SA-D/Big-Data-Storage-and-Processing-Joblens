"""Kiểm tra độ tươi của gold.mart_salary_role_region sau khi DAG xây gold và chạy bảo trì.

Job nhỏ chạy bằng Spark, đọc thời điểm làm mới gần nhất từ metadata Iceberg (.history, dự phòng .snapshots)
và `max(month)` của mart để log đối chiếu; không tạo bảng mới, không ghi dữ liệu.

Ngưỡng mặc định warn 90 phút / error 180 phút là **ngưỡng chính sách** do nhóm đặt, không phải số đo p95.
Cấu hình qua JOBLENS_FRESHNESS_WARN_MINUTES / JOBLENS_FRESHNESS_ERROR_MINUTES (cùng biến với GX và dbt source).
Quá ngưỡng warn thì log cảnh báo; quá ngưỡng error (hoặc không đọc được thời điểm làm mới) thì exit 1 để DAG đỏ.

Chạy: bash deploy/spark/submit.sh gold_freshness_check [--table lake.gold.mart_salary_role_region]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

import bronze_jobs

CATALOG = bronze_jobs.CATALOG
DEFAULT_TABLE = f"{CATALOG}.gold.mart_salary_role_region"
DEFAULT_WARN_MINUTES = 90
DEFAULT_ERROR_MINUTES = 180

log = logging.getLogger("gold_freshness_check")


def thresholds() -> tuple[int, int]:
    """Ngưỡng chính sách tính bằng phút; không phải số đo."""
    warn = int(
        os.environ.get("JOBLENS_FRESHNESS_WARN_MINUTES", str(DEFAULT_WARN_MINUTES))
    )
    error = int(
        os.environ.get("JOBLENS_FRESHNESS_ERROR_MINUTES", str(DEFAULT_ERROR_MINUTES))
    )
    if warn <= 0 or error <= 0 or warn > error:
        raise ValueError(
            f"Ngưỡng độ tươi không hợp lệ: warn={warn}, error={error} (phút)"
        )
    return warn, error


def _as_utc_naive(value: datetime) -> datetime:
    return value.replace(tzinfo=None) if value.tzinfo else value


def evaluate(last_refresh: datetime, now: datetime, warn: int, error: int) -> dict:
    """Tuổi của lần làm mới gold gần nhất, so với ngưỡng chính sách."""
    age_minutes = (
        _as_utc_naive(now) - _as_utc_naive(last_refresh)
    ).total_seconds() / 60
    return {
        "age_minutes": age_minutes,
        "warn": age_minutes > warn,
        "success": age_minutes <= error,
    }


def metadata_sources(table: str) -> list[tuple[str, str]]:
    """Bảng metadata Iceberg cần đọc và cột thời điểm tương ứng."""
    namespace, name = table.rsplit(".", 1)
    return [
        (f"{namespace}.{name}.history", "made_current_at"),
        (f"{namespace}.{name}.snapshots", "committed_at"),
        (f"{namespace}.`{name}$history`", "made_current_at"),
        (f"{namespace}.`{name}$snapshots`", "committed_at"),
    ]


def last_refresh(spark, table: str) -> tuple[datetime | None, str | None]:
    """Thời điểm commit gần nhất; thử .history rồi .snapshots, bỏ qua nguồn không đọc được."""
    problems = []
    for source, column in metadata_sources(table):
        try:
            value = spark.sql(f"SELECT max({column}) FROM {source}").first()[0]
            if value is not None:
                return value, f"{source}.{column}"
        except Exception as exc:
            problems.append(f"{source}: {exc}")
    if problems:
        log.warning("Không đọc được một số nguồn metadata: %s", "; ".join(problems))
    return None, None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Kiểm tra độ tươi của gold theo ngưỡng chính sách"
    )
    p.add_argument(
        "--table",
        default=os.environ.get("JOBLENS_GOLD_TABLE", DEFAULT_TABLE),
        help="Bảng gold cần kiểm tra (mặc định lake.gold.mart_salary_role_region)",
    )
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    warn, error = thresholds()
    spark = bronze_jobs.build_session("gold-freshness-check")
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        max_month = spark.sql(f"SELECT max(month) FROM {args.table}").first()[0]
        value, source = last_refresh(spark, args.table)
        log.info(
            "Kiểm tra độ tươi gold: now(UTC)=%s, bảng=%s, max(month)=%s, nguồn thời điểm=%s",
            now.isoformat(),
            args.table,
            max_month,
            source,
        )
        if value is None:
            log.error(
                "Không có thời điểm làm mới gold trong metadata – không chứng minh được độ tươi"
            )
            return 1
        result = evaluate(value, now, warn, error)
        log.info(
            "Lần làm mới gold gần nhất=%s; tuổi=%.1f phút; ngưỡng chính sách warn=%s phút, error=%s phút",
            value.isoformat(),
            result["age_minutes"],
            warn,
            error,
        )
        if result["warn"]:
            log.warning("Gold vượt ngưỡng warn %s phút", warn)
        if not result["success"]:
            log.error("Gold vượt ngưỡng error %s phút – dừng pipeline", error)
            return 1
        return 0
    except Exception as exc:
        log.error("Lỗi khi kiểm tra độ tươi gold: %s", exc)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
