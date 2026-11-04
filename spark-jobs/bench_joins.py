"""Thí nghiệm tối ưu nhiều join (kỹ năng Spark nhóm 3 của IT4931: "tối ưu nhiều join").

Cùng một truy vấn 4 bảng chạy dưới nhiều cấu hình, đo thời gian và đếm loại join / số lần shuffle trong kế hoạch
thực thi CUỐI (sau khi chạy – với AQE là kế hoạch đã tối ưu lại lúc chạy):

  silver.job_postings  (một dòng mỗi tin)
    ⋈ silver.job_postings_snapshot gộp theo job_uid   (số ngày xuất hiện – bảng lớn)
    ⋈ bronze.jobs_raw gộp theo job_uid                (số lần crawl – bảng lớn nhất)
    ⋈ danh mục 34 tỉnh (sau explode location_codes)    (bảng rất nhỏ, dữ liệu lệch: Hà Nội/TP.HCM chiếm phần lớn)
  → gộp theo vùng × cấp bậc

Cấu hình:
  smj_only      AQE tắt, tắt broadcast (autoBroadcastJoinThreshold = -1): mọi join là sort-merge – mốc so sánh
  broadcast     AQE tắt, broadcast theo ngưỡng mặc định 10 MB. Bộ lập kế hoạch tĩnh chỉ broadcast khi ƯỚC LƯỢNG
                kích thước < ngưỡng: bảng Iceberg có thống kê file nên ước lượng được; bảng tạo từ Python
                (createDataFrame) không có thống kê nên bị ước lượng rất lớn và vẫn sort-merge – AQE (đo kích thước
                thật lúc chạy) hoặc gợi ý broadcast tường minh mới đổi được
  aqe           AQE bật (gộp partition nhỏ sau shuffle, đổi chiến lược join lúc chạy, tách partition lệch)
  aqe_reorder   AQE bật + viết lại truy vấn: gộp và lọc trước khi join, join bảng nhỏ trước, gợi ý broadcast

Mỗi cấu hình: 1 lần chạy làm nóng + REPEAT lần đo (lấy trung vị). Ghi bảng markdown ra log và
s3://joblens/reports/join-bench/<thời điểm>.json. Không ghi bảng nào.

Chạy:  EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh bench_joins [--repeat 3]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import statistics
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("bench_joins")

CONFIGS = {
    "smj_only": {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    },
    "broadcast": {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "10485760",
    },
    "aqe": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "10485760",
    },
    "aqe_reorder": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "10485760",
    },
}
RESULT_PREFIX = "s3://joblens/reports/join-bench"


def query(
    silver: DataFrame,
    snapshot: DataFrame,
    bronze: DataFrame,
    dim: DataFrame,
    reorder: bool,
) -> DataFrame:
    """Truy vấn nhiều join. reorder=False: viết "ngây thơ" (join bảng thô rồi mới gộp).
    reorder=True: gộp/lọc trước, join bảng nhỏ trước, broadcast tường minh."""
    if not reorder:
        jobs = silver.select(
            "job_uid",
            "level_code",
            "location_codes",
            "salary_min_vnd",
            "salary_max_vnd",
            "source",
        )
        j = (
            jobs.join(snapshot.select("job_uid", "snapshot_date"), "job_uid")
            .join(bronze.select("job_uid", "kafka_offset"), "job_uid")
            .select("*", F.explode("location_codes").alias("location_code"))
            .join(dim, "location_code")
            .filter(F.col("source") != "synthetic")
        )
        return j.groupBy("region", "level_code").agg(
            F.countDistinct("job_uid").alias("jobs"),
            F.avg((F.col("salary_min_vnd") + F.col("salary_max_vnd")) / 2).alias(
                "avg_salary"
            ),
            F.countDistinct("job_uid", "snapshot_date").alias("job_days"),
            F.count("kafka_offset").alias("rows_joined"),
        )
    jobs = silver.filter(F.col("source") != "synthetic").select(
        "job_uid",
        "level_code",
        "location_codes",
        ((F.col("salary_min_vnd") + F.col("salary_max_vnd")) / 2).alias("mid"),
    )
    loc = jobs.select("*", F.explode("location_codes").alias("location_code")).join(
        F.broadcast(dim), "location_code"
    )
    days = snapshot.groupBy("job_uid").agg(
        F.countDistinct("snapshot_date").alias("days")
    )
    crawls = bronze.groupBy("job_uid").agg(F.count("*").alias("crawls"))
    j = loc.join(days, "job_uid").join(crawls, "job_uid")
    return j.groupBy("region", "level_code").agg(
        F.countDistinct("job_uid").alias("jobs"),
        F.avg("mid").alias("avg_salary"),
        F.sum("days").alias("job_days"),
        F.sum(F.col("days") * F.col("crawls")).alias("rows_joined"),
    )


def plan_counts(plan: str) -> dict:
    """Đếm toán tử trong kế hoạch thực thi (chuỗi): loại join, số Exchange (shuffle), có phải kế hoạch cuối của AQE.
    Với AQE, chuỗi kế hoạch có cả "Final Plan" lẫn "Initial Plan" – chỉ đếm phần kế hoạch cuối."""
    final = plan.split("== Initial Plan ==")[0]
    aqe_final = "isFinalPlan=true" in plan
    plan = final
    return {
        "broadcast_hash_join": len(re.findall(r"BroadcastHashJoin", plan)),
        "sort_merge_join": len(re.findall(r"SortMergeJoin", plan)),
        "shuffle_exchange": len(
            re.findall(
                r"(?<!Broadcast)Exchange (hashpartitioning|rangepartitioning|SinglePartition|RoundRobin)",
                plan,
            )
        ),
        "aqe_final": aqe_final,
    }


def executed_plan(df: DataFrame) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def run_config(spark: SparkSession, name: str, tables: tuple, repeat: int) -> dict:
    for k, v in CONFIGS[name].items():
        spark.conf.set(k, v)
    times = []
    plan = ""
    rows = None
    for i in range(repeat + 1):  # lần 0 làm nóng (đọc metadata, JIT)
        df = query(*tables, reorder=(name == "aqe_reorder"))
        t0 = time.perf_counter()
        out = df.collect()
        dt = time.perf_counter() - t0
        if i > 0:
            times.append(dt)
        if i == repeat:
            plan = executed_plan(df)
            rows = sorted((r["region"], r["level_code"], r["jobs"]) for r in out)
    return {
        "config": name,
        "seconds_median": round(statistics.median(times), 2),
        "seconds_min": round(min(times), 2),
        "seconds_max": round(max(times), 2),
        **plan_counts(plan),
        "result_rows": len(rows),
        "_result": rows,
        "_plan": plan,
    }


def table_md(results: list[dict]) -> str:
    cols = ["config", "seconds_median", "seconds_min", "seconds_max", "broadcast_hash_join", "sort_merge_join",
            "shuffle_exchange", "aqe_final", "result_rows"]  # fmt: skip
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in results]
    return "\n".join(lines)


def same_results(results: list[dict]) -> bool:
    """Mọi cấu hình phải cho cùng số tin theo vùng × cấp bậc (tối ưu không được đổi kết quả)."""
    return len({json.dumps(r["_result"]) for r in results}) == 1


def _upload(results: list[dict], now: datetime) -> str:
    import boto3
    from botocore.config import Config

    uri = f"{RESULT_PREFIX}/{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    body = {
        "run_at": now.isoformat(),
        "results": [{k: v for k, v in r.items() if k != "_result"} for r in results],
    }
    boto3.client(
        "s3",
        endpoint_url=os.environ["JOBLENS_S3_ENDPOINT"],
        config=Config(s3={"addressing_style": "path"}),
    ).put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(body, ensure_ascii=False, indent=2).encode(),
    )
    return uri


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Thí nghiệm tối ưu nhiều join")
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--configs", default=",".join(CONFIGS))
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    import analytics_spark
    import bronze_jobs

    spark = bronze_jobs.build_session("bench-joins")
    cat = bronze_jobs.CATALOG
    tables = (
        spark.table(f"{cat}.silver.job_postings"),
        spark.table(f"{cat}.silver.job_postings_snapshot"),
        spark.table(f"{cat}.bronze.jobs_raw").withColumn(
            "job_uid", F.sha1(F.concat_ws(":", "source", "source_job_id"))
        ),
        analytics_spark.dim_locations(spark),
    )
    log.info(
        "kích thước: silver %s, snapshot %s, bronze %s dòng",
        *(t.count() for t in tables[:3]),
    )
    results = [
        run_config(spark, c, tables, args.repeat) for c in args.configs.split(",")
    ]
    log.info("Kết quả (giây, trung vị %s lần đo):\n%s", args.repeat, table_md(results))
    log.info("Mọi cấu hình cho cùng kết quả: %s", same_results(results))
    for r in results:
        log.info("Kế hoạch cuối – %s:\n%s", r["config"], r["_plan"][:4000])
    if not args.no_upload:
        log.info("đã ghi %s", _upload(results, datetime.now(timezone.utc)))


if __name__ == "__main__":
    main()
