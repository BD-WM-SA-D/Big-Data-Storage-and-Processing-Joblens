"""Streaming có trạng thái: số tin mới theo tỉnh trong từng cửa sổ thời gian, ghi vào MongoDB.

Kafka jobs.raw.*.v1 → parse (cùng hàm với bronze) → chuẩn hóa địa điểm + lương (cùng UDF với silver)
→ withWatermark(crawl_ts) → groupBy(window(crawl_ts), location_code) → foreachBatch upsert MongoDB
(collection joblens.province_windows).

Thể hiện các yêu cầu Spark Structured Streaming của môn học:
* **Event time + watermark:** cửa sổ theo `crawl_ts` (thời điểm crawler lấy tin), không theo lúc Spark nhận.
  Tin đến trễ hơn `--watermark` so với tin mới nhất đã thấy bị bỏ (Spark đếm ở numRowsDroppedByWatermark,
  job in ra log mỗi micro-batch). Watermark cũng cho phép Spark xóa state của cửa sổ cũ.
* **State:** phép groupBy theo cửa sổ giữ state (số tin, tổng lương) giữa các micro-batch, lưu trong checkpoint.
* **Output mode:** `update` (mặc định) – mỗi micro-batch ghi các cửa sổ vừa thay đổi, dashboard thấy số tăng dần;
  `append` – chỉ ghi một lần khi cửa sổ đã đóng (watermark vượt qua cuối cửa sổ), kết quả cuối không đổi nữa.
* **Exactly-once (về hiệu quả):** Kafka offset + state nằm trong checkpoint; sink upsert theo _id =
  "<đầu cửa sổ>|<mã tỉnh>" với giá trị tuyệt đối (không cộng dồn) → chạy lại một micro-batch sau sự cố
  cho cùng kết quả (idempotent).

Chạy:  bash deploy/spark/submit.sh stream_province_windows [--window "5 minutes"] [--watermark "10 minutes"]
       [--trigger "10 seconds"] [--output-mode update|append]
Tạo tin trễ để xem watermark hoạt động:  bash deploy/data/replay-vietjobs.sh 3000 50 --late-every 10
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import bronze_jobs
from silver_job_postings import normalize_udf

MONGO_DB = "joblens"
COLLECTION = "province_windows"
UNKNOWN = "unknown"
# Lược đồ cho catalog mongodb của Trino. Trino tự đoán cột từ document mẫu rồi LƯU vào collection _schema;
# nếu lần đầu Grafana hỏi lúc collection còn rỗng thì Trino lưu lược đồ chỉ có _id và không đoán lại nữa
# (lỗi "Column 'window_start' cannot be resolved"). Vì vậy job tự ghi lược đồ đúng khi khởi động.
TRINO_FIELDS = [
    ("_id", "varchar"),
    ("window_start", "timestamp(3)"),
    ("window_end", "timestamp(3)"),
    ("location_code", "varchar"),
    ("postings", "bigint"),
    ("salary_postings", "bigint"),
    ("avg_salary_mid_vnd", "bigint"),
    ("sources", "bigint"),
    ("batch_id", "bigint"),
    ("updated_at", "timestamp(3)"),
]

log = logging.getLogger("stream_province_windows")


def postings_events(raw: DataFrame) -> DataFrame:
    """Dòng Kafka → một dòng cho mỗi (tin, tỉnh): crawl_ts, location_code, salary_mid_vnd, source."""
    rows = bronze_jobs.to_bronze(raw).filter(F.col("_valid"))
    n = rows.withColumn("n", normalize_udf("payload_json"))
    smin, smax = F.col("n.salary_min_vnd"), F.col("n.salary_max_vnd")
    mid = (
        F.when(smin.isNotNull() & smax.isNotNull(), (smin + smax) / 2)
        .otherwise(F.coalesce(smin, smax))
        .cast("double")
    )
    locs = F.when(F.size("n.location_codes") > 0, F.col("n.location_codes")).otherwise(
        F.array(F.lit(UNKNOWN))
    )
    return n.select(
        "crawl_ts",
        "source",
        F.explode(locs).alias("location_code"),
        mid.alias("salary_mid_vnd"),
    )


def windowed_counts(events: DataFrame, window: str, watermark: str) -> DataFrame:
    """Aggregation có state theo cửa sổ sự kiện. Chạy được cả trên DataFrame batch (để kiểm thử)."""
    return (
        events.withWatermark("crawl_ts", watermark)
        .groupBy(F.window("crawl_ts", window).alias("w"), "location_code")
        .agg(
            F.count("*").alias("postings"),
            F.count("salary_mid_vnd").alias("salary_postings"),
            F.sum("salary_mid_vnd").alias("salary_mid_sum_vnd"),
            F.approx_count_distinct("source").alias("sources"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "location_code",
            "postings",
            "salary_postings",
            "salary_mid_sum_vnd",
            "sources",
        )
    )


def to_documents(rows: list[dict], batch_id: int, now: datetime) -> list[dict]:
    """Dòng kết quả → document MongoDB. _id cố định theo (cửa sổ, tỉnh) để upsert idempotent."""
    docs = []
    for r in rows:
        start = r["window_start"]
        avg = (
            r["salary_mid_sum_vnd"] / r["salary_postings"]
            if r["salary_postings"]
            else None
        )
        docs.append(
            {
                "_id": f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}|{r['location_code']}",
                "window_start": start,
                "window_end": r["window_end"],
                "location_code": r["location_code"],
                "postings": int(r["postings"]),
                "salary_postings": int(r["salary_postings"]),
                "avg_salary_mid_vnd": round(avg) if avg is not None else None,
                "sources": int(r["sources"]),
                "batch_id": int(batch_id),
                "updated_at": now,
            }
        )
    return docs


def trino_schema_doc(collection: str = COLLECTION) -> dict:
    """Document cho collection _schema mà connector MongoDB của Trino đọc để biết cột của bảng."""
    return {
        "table": collection,
        "fields": [{"name": n, "type": t, "hidden": False} for n, t in TRINO_FIELDS],
    }


def mongo_writer(url: str, collection: str = COLLECTION):
    """Hàm foreachBatch: kết quả mỗi micro-batch nhỏ (≤ số tỉnh × số cửa sổ đang mở) nên gom về driver
    rồi bulk upsert bằng pymongo – không cần connector Spark–MongoDB."""
    from pymongo import MongoClient, ReplaceOne

    client = MongoClient(url, tz_aware=True)
    coll = client.get_default_database(MONGO_DB)[collection]
    coll.create_index([("window_start", 1), ("location_code", 1)])
    coll.database["_schema"].replace_one(
        {"table": collection}, trino_schema_doc(collection), upsert=True
    )

    def write(batch: DataFrame, batch_id: int) -> None:
        rows = [r.asDict() for r in batch.collect()]
        docs = to_documents(rows, batch_id, datetime.now(timezone.utc))
        if docs:
            res = coll.bulk_write(
                [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs]
            )
            log.info(
                "batch %s: %s cửa sổ×tỉnh (mới %s, cập nhật %s)",
                batch_id,
                len(docs),
                res.upserted_count,
                res.modified_count,
            )

    return write


def log_progress(progress: dict) -> None:
    """In một dòng cho mỗi stateful operator của một micro-batch.

    numRowsDroppedByWatermark đếm dòng ở đầu vào của phép gộp có state – sau bước gộp một phần, tức là theo
    (cửa sổ, tỉnh) chứ không theo từng tin."""
    for op in progress.get("stateOperators", []):
        log.info(
            "tiến độ batch %s: vào %s dòng; state %s dòng; bỏ do trễ (watermark) %s; watermark %s",
            progress.get("batchId"),
            progress.get("numInputRows"),
            op.get("numRowsTotal"),
            op.get("numRowsDroppedByWatermark"),
            progress.get("eventTime", {}).get("watermark"),
        )


def new_progress(recent: list[dict], last: int | None) -> list[dict]:
    """Các bản tiến độ có batchId > last (recentProgress giữ vài batch gần nhất; một vòng chờ có thể qua nhiều batch)."""
    return [p for p in recent if last is None or p.get("batchId", -1) > last]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--window", default="5 minutes")
    p.add_argument("--watermark", default="10 minutes")
    p.add_argument("--trigger", default="10 seconds")
    p.add_argument("--output-mode", choices=["update", "append"], default="update")
    p.add_argument("--starting-offsets", default="latest")
    p.add_argument(
        "--once",
        action="store_true",
        help="Xử lý dữ liệu đang có rồi dừng (availableNow)",
    )
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    spark: SparkSession = bronze_jobs.build_session("stream-province-windows")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["JOBLENS_KAFKA_BOOTSTRAP"])
        .option("subscribePattern", bronze_jobs.TOPIC_PATTERN)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    counts = windowed_counts(postings_events(raw), args.window, args.watermark)
    ckpt = os.path.join(
        os.environ.get("JOBLENS_CHECKPOINT_DIR", "/checkpoints"),
        f"province_windows_{args.output_mode}",
    )
    writer = (
        counts.writeStream.outputMode(args.output_mode)
        .foreachBatch(mongo_writer(os.environ["JOBLENS_MONGO_URL"]))
        .option("checkpointLocation", ckpt)
    )
    writer = (
        writer.trigger(availableNow=True)
        if args.once
        else writer.trigger(processingTime=args.trigger)
    )
    query = writer.start()
    log.info(
        "bắt đầu: cửa sổ %s, watermark %s, output mode %s, checkpoint %s",
        args.window,
        args.watermark,
        args.output_mode,
        ckpt,
    )
    last = None
    while query.isActive:
        query.awaitTermination(15)
        for prog in new_progress(query.recentProgress, last):
            log_progress(prog)
            last = prog.get("batchId")
    if query.exception():
        raise query.exception()


if __name__ == "__main__":
    main()
