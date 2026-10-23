"""Cổng chất lượng dữ liệu cho silver.job_postings bằng Great Expectations (GX Core 1.23) trên Spark.

Chạy trong DAG joblens_pipeline ngay sau silver_job_postings, trước dbt: có kỳ vọng nào hỏng thì task đỏ và
gold không được xây lại (docs/project-plan/03: "GE + dbt tests gate publish"). Hai lớp kiểm tra:

* **Ràng buộc cứng** (phải đúng 100%): job_uid không trùng/không null, nguồn hợp lệ, mã cấp bậc/loại hình
  thuộc danh mục, lương nằm trong khoảng hợp lý và max ≥ min, mã tỉnh thuộc 34 mã sau sắp xếp 2025.
* **Độ phủ** (`mostly`): tỉ lệ tin có địa điểm ≥ 90%, có lương ≥ 50%. Ngưỡng là chỉ tiêu nhóm đặt ra, ghi ở
  THRESHOLDS – đổi ngưỡng phải ghi lý do vào commit.

Kết quả (JSON của GX) ghi vào s3://joblens/quality/gx/silver_job_postings/<thời điểm>.json để tra lại, và in
bảng tóm tắt ra log (thấy trên giao diện Airflow).

Chạy tay:  bash deploy/spark/submit.sh gx_silver [--no-fail]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")
os.environ.setdefault(
    "TQDM_DISABLE", "1"
)  # GX vẽ thanh tiến trình cho từng metric – làm rối log Airflow

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import normalize as N  # noqa: E402

SOURCES = ["topcv", "itviec", "vietjobs", "synthetic"]
THRESHOLDS = {"has_location": 0.90, "has_salary": 0.50}
RESULT_PREFIX = "s3://joblens/quality/gx/silver_job_postings"

log = logging.getLogger("gx_silver")


def location_codes(csv_path: str | None = None) -> list[str]:
    path = csv_path or N._default_locations_csv()  # cùng file với chuẩn hóa và dbt seed
    with open(path, encoding="utf-8") as f:
        return sorted(r["location_code"] for r in csv.DictReader(f))


def prepare(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(bảng tin + cột phụ has_location/has_salary, bảng mã tỉnh đã tách từ mảng location_codes)."""
    jobs = silver.withColumn("has_location", F.size("location_codes") > 0).withColumn(
        "has_salary",
        F.col("salary_min_vnd").isNotNull() | F.col("salary_max_vnd").isNotNull(),
    )
    locs = silver.select(F.explode("location_codes").alias("location_code"))
    return jobs, locs


def job_expectations(gxe) -> list:
    """Kỳ vọng trên từng tin. gxe = module great_expectations.expectations (truyền vào để dễ kiểm thử)."""
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="job_uid"),
        gxe.ExpectColumnValuesToBeUnique(column="job_uid"),
        gxe.ExpectColumnValuesToNotBeNull(column="title"),
        gxe.ExpectColumnValuesToBeInSet(column="source", value_set=SOURCES),
        gxe.ExpectColumnValuesToNotBeNull(column="is_active"),
        gxe.ExpectColumnValuesToBeInSet(
            column="level_code", value_set=list(N.LEVEL_CODES)
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="employment_type_code", value_set=list(N.EMPLOYMENT_CODES)
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="salary_min_vnd",
            min_value=N.SALARY_MIN_VALID,
            max_value=N.SALARY_MAX_VALID,
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="salary_max_vnd",
            min_value=N.SALARY_MIN_VALID,
            max_value=N.SALARY_MAX_VALID,
        ),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="salary_max_vnd",
            column_B="salary_min_vnd",
            or_equal=True,
            ignore_row_if="either_value_is_missing",
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="experience_min_years", min_value=0, max_value=40
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="has_location", value_set=[True], mostly=THRESHOLDS["has_location"]
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="has_salary", value_set=[True], mostly=THRESHOLDS["has_salary"]
        ),
    ]


def location_expectations(gxe, codes: list[str]) -> list:
    return [gxe.ExpectColumnValuesToBeInSet(column="location_code", value_set=codes)]


def validate(silver: DataFrame, codes: list[str]) -> list[dict]:
    """Chạy hai bộ kỳ vọng, trả danh sách kết quả gọn: name, column, success, unexpected_percent, observed."""
    import great_expectations as gx
    import great_expectations.expectations as gxe

    jobs, locs = prepare(silver)
    from great_expectations.data_context.types.base import ProgressBarsConfig

    ctx = gx.get_context(mode="ephemeral")
    # tắt thanh tiến trình "Calculating Metrics" (biến TQDM_DISABLE không đủ – GX tự quyết định hiển thị)
    ctx.variables.progress_bars = ProgressBarsConfig(
        globally=False, metric_calculations=False
    )
    ds = ctx.data_sources.add_spark("spark")
    out: list[dict] = []
    for name, df, expectations in [
        ("silver_job_postings", jobs, job_expectations(gxe)),
        ("silver_location_codes", locs, location_expectations(gxe, codes)),
    ]:
        batch_def = ds.add_dataframe_asset(name).add_batch_definition_whole_dataframe(
            "all"
        )
        suite = ctx.suites.add(gx.ExpectationSuite(name))
        for e in expectations:
            suite.add_expectation(e)
        res = batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)
        for r in res.results:
            kw = r.expectation_config.kwargs
            out.append(
                {
                    "suite": name,
                    "expectation": r.expectation_config.type,
                    "column": kw.get("column")
                    or (
                        f"{kw['column_A']} ≥ {kw['column_B']}"
                        if "column_A" in kw
                        else ""
                    ),
                    "mostly": kw.get("mostly", 1.0),
                    "success": bool(r.success),
                    "unexpected_percent": r.result.get("unexpected_percent"),
                    "observed": r.result.get("observed_value"),
                }
            )
    return out


def summary_table(results: list[dict]) -> str:
    lines = [f"{'KQ':4} {'kỳ vọng':48} {'cột':34} {'% sai':>8}  ngưỡng"]
    for r in results:
        pct = r["unexpected_percent"]
        lines.append(
            f"{'ĐẠT' if r['success'] else 'HỎNG':4} {r['expectation']:48} {str(r['column'])[:34]:34} "
            f"{'' if pct is None else f'{pct:.2f}':>8}  {'' if r['mostly'] == 1.0 else r['mostly']}"
            + ("" if r["observed"] is None else f"  (giá trị: {r['observed']})")
        )
    ok = sum(r["success"] for r in results)
    lines.append(f"→ {ok}/{len(results)} kỳ vọng đạt")
    return "\n".join(lines)


def _upload(results: list[dict], now: datetime) -> str:
    import boto3
    from botocore.config import Config

    uri = f"{RESULT_PREFIX}/{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["JOBLENS_S3_ENDPOINT"],
        config=Config(s3={"addressing_style": "path"}),
    )
    body = {
        "validated_at": now.isoformat(),
        "table": "silver.job_postings",
        "results": results,
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(body, ensure_ascii=False, default=str, indent=2).encode(),
        ContentType="application/json",
    )
    return uri


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Kiểm tra chất lượng silver.job_postings bằng GX"
    )
    p.add_argument(
        "--no-fail",
        action="store_true",
        help="Chỉ báo cáo, không trả mã lỗi khi có kỳ vọng hỏng",
    )
    p.add_argument("--no-upload", action="store_true", help="Không ghi kết quả lên S3")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    import bronze_jobs

    spark = bronze_jobs.build_session("gx-silver")
    silver = spark.table(f"{bronze_jobs.CATALOG}.silver.job_postings")
    now = datetime.now(timezone.utc)
    results = validate(silver, location_codes())
    log.info("Kết quả GX trên silver.job_postings:\n%s", summary_table(results))
    if not args.no_upload:
        log.info("đã ghi %s", _upload(results, now))
    failed = [r for r in results if not r["success"]]
    if failed and not args.no_fail:
        log.error("%s kỳ vọng hỏng – dừng pipeline trước khi xây gold", len(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
