"""Phân tích thị trường việc làm bằng Spark – thể hiện các kỹ thuật Spark mà môn học yêu cầu, ghi kết quả vào
MongoDB (collection analytics_*) và kế hoạch thực thi vào s3://joblens/reports/spark-plans/<thời điểm>/.

| Yêu cầu môn học                     | Ở đâu trong job                                                         |
|-------------------------------------|-------------------------------------------------------------------------|
| Window functions                    | xếp hạng tỉnh theo lương trong vùng, tỉ trọng cộng dồn, trung bình trượt |
| Pivot / unpivot                     | số tin vùng × cấp bậc (pivot), rồi unpivot lại để đối chiếu tổng         |
| Hàm tổng hợp tùy biến               | pandas UDF tính trung bình cắt 10% (trimmed mean) lương theo tỉnh       |
| Biến đổi nhiều bước, UDF            | explode địa điểm → join → tổng hợp; UDF chuẩn hóa nằm ở job silver      |
| Broadcast join (dữ liệu lệch)       | tin ⋈ 34 tỉnh: bảng nhỏ broadcast; Hà Nội/TP.HCM chiếm phần lớn tin    |
| Sort-merge join                     | silver ⋈ snapshot theo job_uid (hai bảng lớn, gợi ý MERGE)              |
| Partition pruning                   | lọc bronze theo ingest_ts (phân vùng days(ingest_ts)) – xem plan        |
| Cache / persist                     | bảng tin đã explode dùng lại cho nhiều phép tổng hợp                    |
| Phân tích kế hoạch thực thi         | explain("formatted") của từng bước lưu lên S3 + cờ kiểm tra trong log   |
| Thống kê, chuỗi thời gian           | tương quan kinh nghiệm–lương, phân vị; lượng nạp theo giờ + trung bình trượt |
| Spark MLlib                         | Pipeline + CrossValidator hồi quy log(lương), so với mô hình trung vị   |

Chạy:  bash deploy/spark/submit.sh analytics_spark [--no-mongo] [--no-upload]   (DAG: task spark_analytics)
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import logging
import math
import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

import normalize as N

MONGO_DB = "joblens"
PLAN_PREFIX = "s3://joblens/reports/spark-plans"
LEVELS = [c for c in N.LEVEL_CODES if c != "other"]

log = logging.getLogger("analytics_spark")


# ------------------------------------------------------------------ tiện ích
def plan_text(df: DataFrame, mode: str = "formatted") -> str:
    """Kế hoạch thực thi dạng chữ (DataFrame.explain in ra stdout của Python)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def dim_locations(spark: SparkSession, csv_path: str | None = None) -> DataFrame:
    path = csv_path or str(N._default_locations_csv())
    with open(path, encoding="utf-8") as f:
        rows = [(r["location_code"], r["name"], r["region"]) for r in csv.DictReader(f)]
    return spark.createDataFrame(
        rows, "location_code string, name string, region string"
    )


@pandas_udf(
    T.DoubleType()
)  # kiểu đối tượng, không phải chuỗi DDL: import module không cần SparkSession
def trimmed_mean(v: pd.Series) -> float:
    """Hàm tổng hợp tùy biến: trung bình sau khi bỏ 10% thấp nhất và 10% cao nhất (bớt nhạy với tin lương ảo)."""
    v = v.dropna().sort_values()
    if v.empty:
        return float("nan")
    k = int(len(v) * 0.1)
    core = v.iloc[k : len(v) - k] if len(v) - 2 * k > 0 else v
    return float(core.mean())


# ------------------------------------------------------------------ các bước phân tích
def job_rows(silver: DataFrame) -> DataFrame:
    """Một dòng mỗi tin (trừ synthetic) + lương trung điểm."""
    smin, smax = F.col("salary_min_vnd"), F.col("salary_max_vnd")
    mid = F.when(smin.isNotNull() & smax.isNotNull(), (smin + smax) / 2).otherwise(
        F.coalesce(smin, smax)
    )
    return silver.filter(F.col("source") != "synthetic").withColumn(
        "salary_mid_vnd", mid.cast("double")
    )


def explode_locations(jobs: DataFrame, dim: DataFrame) -> DataFrame:
    """Tin × tỉnh, join với danh mục tỉnh bằng broadcast join (34 dòng) – không shuffle bảng tin lớn."""
    exploded = jobs.select(
        "job_uid", "level_code", "salary_mid_vnd", "experience_min_years",
        F.explode("location_codes").alias("location_code"),
    )  # fmt: skip
    return exploded.join(F.broadcast(dim), "location_code", "inner")


def salary_by_province(loc: DataFrame) -> DataFrame:
    """Tổng hợp theo tỉnh + window functions: hạng lương trong vùng, tỉ trọng tin cộng dồn toàn quốc."""
    keys = ["location_code", "name", "region"]
    builtin = loc.groupBy(*keys).agg(
        F.countDistinct("job_uid").alias("postings"),
        F.count("salary_mid_vnd").alias("salary_postings"),
        F.percentile_approx("salary_mid_vnd", 0.5).alias("median_salary_vnd"),
    )
    # Spark không cho trộn pandas UDF tổng hợp với hàm tổng hợp có sẵn trong cùng một agg → tính riêng rồi join
    custom = loc.groupBy(*keys).agg(
        trimmed_mean("salary_mid_vnd").alias("trimmed_mean_salary_vnd")
    )
    agg = builtin.join(custom, keys)
    by_region = Window.partitionBy("region").orderBy(F.desc("median_salary_vnd"))
    by_volume = Window.orderBy(F.desc("postings")).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    total = Window.partitionBy()
    return (
        agg.withColumn("rank_in_region", F.dense_rank().over(by_region))
        .withColumn(
            "share_of_postings", F.col("postings") / F.sum("postings").over(total)
        )
        .withColumn(
            "cumulative_share",
            F.sum("postings").over(by_volume) / F.sum("postings").over(total),
        )
    )


def level_region_pivot(loc: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Pivot: dòng = vùng, cột = cấp bậc (số tin). Unpivot lại thành dạng dài để đối chiếu."""
    pivot = (
        loc.filter(F.col("level_code").isin(LEVELS))
        .groupBy("region")
        .pivot(
            "level_code", LEVELS
        )  # liệt kê giá trị trước → Spark không phải quét thêm một lượt để tìm cột
        .agg(F.countDistinct("job_uid"))
        .na.fill(0)
    )
    long = pivot.unpivot("region", LEVELS, "level_code", "postings")
    return pivot, long


def activity_join(jobs: DataFrame, snapshot: DataFrame) -> DataFrame:
    """Sort-merge join hai bảng lớn theo job_uid (gợi ý MERGE): số ngày tin xuất hiện, số ngày còn mở."""
    days = snapshot.groupBy("job_uid").agg(
        F.count("*").alias("days_seen"),
        F.sum(F.col("is_active").cast("int")).alias("days_active"),
    )
    return jobs.select("job_uid", "level_code").join(
        days.hint("merge"), "job_uid", "inner"
    )


def ingest_hourly(bronze: DataFrame, since: datetime) -> DataFrame:
    """Chuỗi thời gian lượng nạp bronze theo giờ + trung bình trượt 3 giờ. Lọc theo ingest_ts → partition pruning."""
    hourly = (
        bronze.filter(F.col("ingest_ts") >= F.lit(since))
        .groupBy(F.date_trunc("hour", "ingest_ts").alias("hour"), "kafka_topic")
        .agg(F.count("*").alias("rows"))
    )
    w = Window.partitionBy("kafka_topic").orderBy("hour").rowsBetween(-2, 0)
    return hourly.withColumn("rows_avg_3h", F.avg("rows").over(w))


def statistics(jobs: DataFrame) -> dict:
    with_salary = jobs.filter(
        F.col("salary_mid_vnd").isNotNull() & F.col("experience_min_years").isNotNull()
    )
    q = with_salary.approxQuantile("salary_mid_vnd", [0.1, 0.25, 0.5, 0.75, 0.9], 0.001)
    return {
        "corr_experience_salary": with_salary.stat.corr(
            "experience_min_years", "salary_mid_vnd"
        ),
        "salary_quantiles_vnd": dict(zip(["p10", "p25", "p50", "p75", "p90"], q)),
        "rows_with_salary_and_experience": with_salary.count(),
    }


def salary_model(loc_jobs: DataFrame, seed: int = 42, folds: int = 3) -> dict:
    """MLlib: dự đoán log(lương trung điểm) từ cấp bậc, vùng, kinh nghiệm. CrossValidator chọn regParam/elasticNet.
    So với mô hình cơ sở "luôn đoán trung vị" để biết mô hình có ích hay không."""
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

    data = (
        loc_jobs.filter(F.col("salary_mid_vnd").isNotNull())
        .groupBy("job_uid")
        .agg(
            F.first("level_code", ignorenulls=True).alias("level_code"),
            F.first("region").alias("region"),
            F.first("experience_min_years", ignorenulls=True).alias(
                "experience_min_years"
            ),
            F.first("salary_mid_vnd").alias("salary_mid_vnd"),
        )
        .na.fill({"level_code": "unknown", "experience_min_years": 0.0})
        .withColumn("label", F.log("salary_mid_vnd"))
    )
    train, test = data.randomSplit([0.8, 0.2], seed=seed)
    stages = [
        StringIndexer(
            inputCols=["level_code", "region"],
            outputCols=["level_i", "region_i"],
            handleInvalid="keep",
        ),
        OneHotEncoder(
            inputCols=["level_i", "region_i"], outputCols=["level_v", "region_v"]
        ),
        VectorAssembler(
            inputCols=["level_v", "region_v", "experience_min_years"],
            outputCol="features",
        ),
    ]
    lr = LinearRegression(featuresCol="features", labelCol="label")
    grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.0, 0.05])
        .addGrid(lr.elasticNetParam, [0.0, 0.5])
        .build()
    )
    rmse_log = RegressionEvaluator(labelCol="label", metricName="rmse")
    cv = CrossValidator(
        estimator=Pipeline(stages=[*stages, lr]),
        estimatorParamMaps=grid,
        evaluator=rmse_log,
        numFolds=folds,
        seed=seed,
        parallelism=1,
    )
    t0 = time.monotonic()
    model = cv.fit(train)
    fit_s = time.monotonic() - t0
    pred = model.transform(test).withColumn("pred_vnd", F.exp("prediction"))
    median_train = train.approxQuantile("salary_mid_vnd", [0.5], 0.001)[0]

    def rmse_m(pred_col):  # RMSE theo triệu VND trên tập kiểm tra
        expr = F.sqrt(F.avg((pred_col - F.col("salary_mid_vnd")) ** 2))
        r = pred.select(expr.alias("r")).first()["r"]
        return None if r is None else r / 1e6

    best = model.bestModel.stages[-1]
    return {
        "train_rows": train.count(),
        "test_rows": test.count(),
        "folds": folds,
        "grid_size": len(grid),
        "best_reg_param": best.getRegParam(),
        "best_elastic_net": best.getElasticNetParam(),
        "cv_rmse_log": min(model.avgMetrics),
        "test_r2_log": RegressionEvaluator(labelCol="label", metricName="r2").evaluate(
            pred
        ),
        "test_rmse_million_vnd": rmse_m(F.col("pred_vnd")),
        "baseline_median_rmse_million_vnd": rmse_m(F.lit(median_train)),
        "fit_seconds": round(fit_s, 1),
    }


def plan_checks(plans: dict[str, str]) -> dict[str, bool]:
    """Cờ kiểm tra kế hoạch thực thi đúng như thiết kế (in vào log, lưu vào analytics_runs)."""
    return {
        "broadcast_join": "BroadcastHashJoin" in plans.get("broadcast_join", ""),
        "sort_merge_join": "SortMergeJoin" in plans.get("sort_merge_join", ""),
        "pivot_has_levels": all(lv in plans.get("pivot", "") for lv in LEVELS),
        # Iceberg đẩy bộ lọc xuống scan ("filters=…ingest_ts…") → chỉ đọc phân vùng days(ingest_ts) cần thiết
        "bronze_filter_pushed": bool(
            re.search(
                r"(filters=|PushedFilters: \[)[^\n]*ingest_ts",
                plans.get("partition_pruning", ""),
            )
        ),
    }


def analyze(silver, snapshot, bronze, dim, since: datetime, run_model: bool = True):
    """Toàn bộ phân tích trên DataFrame đầu vào (để chạy được cả trên dữ liệu kiểm thử)."""
    jobs = job_rows(silver).persist(StorageLevel.MEMORY_AND_DISK)
    loc = explode_locations(jobs, dim).persist(StorageLevel.MEMORY_AND_DISK)
    province = salary_by_province(loc)
    pivot, long = level_region_pivot(loc)
    activity = activity_join(jobs, snapshot)
    hourly = ingest_hourly(bronze, since)
    plans = {
        "broadcast_join": plan_text(explode_locations(jobs, dim)),
        "salary_by_province": plan_text(province),
        "pivot": plan_text(pivot),
        "sort_merge_join": plan_text(activity),
        "partition_pruning": plan_text(hourly),
    }
    pivot_total = sum(sum(r[lv] for lv in LEVELS) for r in pivot.collect())
    long_total = long.agg(F.sum("postings")).first()[0] or 0
    skew = [
        {"location_code": r["location_code"], "rows": r["n"]}
        for r in loc.groupBy("location_code")
        .agg(F.count("*").alias("n"))
        .orderBy(F.desc("n"))
        .limit(3)
        .collect()
    ]
    act = activity.agg(F.avg("days_seen").alias("a"), F.count("*").alias("n")).first()
    summary = {
        "jobs": jobs.count(),
        "job_location_rows": loc.count(),
        "top_locations_rows": skew,
        "pivot_total_equals_unpivot_total": int(pivot_total) == int(long_total),
        "jobs_in_snapshot_join": act["n"],
        "avg_days_seen": act["a"],
        "statistics": statistics(jobs),
        "model": salary_model(loc) if run_model else None,
        "plan_checks": plan_checks(plans),
    }
    outputs = {
        "salary_by_province": province,
        "level_region_pivot": pivot,
        "ingest_hourly": hourly,
    }
    return outputs, summary, plans, (jobs, loc)


# ------------------------------------------------------------------ ghi kết quả
def _clean(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def write_mongo(
    url: str, outputs: dict[str, DataFrame], summary: dict, now: datetime
) -> None:
    from pymongo import MongoClient

    db = MongoClient(url, tz_aware=True).get_default_database(MONGO_DB)
    for name, df in outputs.items():
        docs = [{k: _clean(v) for k, v in r.asDict().items()} for r in df.collect()]
        coll = db[f"analytics_{name}"]
        coll.delete_many(
            {}
        )  # ghi đè toàn bộ mỗi lần chạy (kết quả nhỏ, tính lại từ silver)
        if docs:
            coll.insert_many([{**d, "run_at": now} for d in docs])
        log.info("MongoDB analytics_%s: %s document", name, len(docs))
    db["analytics_runs"].insert_one(
        json.loads(json.dumps({"run_at": now.isoformat(), **summary}, default=str))
    )


def upload_plans(plans: dict[str, str], now: datetime) -> str:
    import boto3
    from botocore.config import Config

    prefix = f"{PLAN_PREFIX}/{now.strftime('%Y%m%dT%H%M%SZ')}"
    bucket, key = prefix.removeprefix("s3://").split("/", 1)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["JOBLENS_S3_ENDPOINT"],
        config=Config(s3={"addressing_style": "path"}),
    )
    for name, text in plans.items():
        s3.put_object(
            Bucket=bucket,
            Key=f"{key}/{name}.txt",
            Body=text.encode(),
            ContentType="text/plain",
        )
    return prefix


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Phân tích thị trường việc làm bằng Spark")
    p.add_argument("--no-mongo", action="store_true")
    p.add_argument("--no-upload", action="store_true")
    p.add_argument("--no-model", action="store_true")
    p.add_argument(
        "--bronze-days",
        type=int,
        default=14,
        help="Chuỗi thời gian lượng nạp: số ngày gần nhất",
    )
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    import bronze_jobs

    spark = bronze_jobs.build_session("analytics-spark")
    spark.conf.set(
        "spark.sql.adaptive.enabled", "true"
    )  # AQE: gộp partition nhỏ, tách partition lệch khi join
    cat = bronze_jobs.CATALOG
    now = datetime.now(timezone.utc)
    since = datetime.fromtimestamp(
        now.timestamp() - args.bronze_days * 86400, timezone.utc
    )
    outputs, summary, plans, cached = analyze(
        spark.table(f"{cat}.silver.job_postings"),
        spark.table(f"{cat}.silver.job_postings_snapshot"),
        spark.table(f"{cat}.bronze.jobs_raw"),
        dim_locations(spark),
        since,
        run_model=not args.no_model,
    )
    log.info(
        "Tóm tắt phân tích:\n%s",
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
    )
    for name, ok in summary["plan_checks"].items():
        log.info("kế hoạch thực thi – %s: %s", name, "ĐÚNG" if ok else "KHÁC THIẾT KẾ")
    log.info("Plan broadcast join:\n%s", plans["broadcast_join"])
    if not args.no_upload:
        log.info("đã lưu kế hoạch thực thi: %s", upload_plans(plans, now))
    if not args.no_mongo:
        write_mongo(os.environ["JOBLENS_MONGO_URL"], outputs, summary, now)
    for df in cached:
        df.unpersist()


if __name__ == "__main__":
    main()
