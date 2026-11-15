"""bronze.jobs_raw → silver.job_postings_history: lịch sử phiên bản của từng tin (SCD loại 2).

silver.job_postings chỉ giữ bản mới nhất của mỗi tin (MERGE ghi đè). Bảng này giữ MỌI trạng thái đã thấy:
mỗi lần nội dung một tin đổi (lương, tiêu đề, cấp bậc, địa điểm…) là một dòng mới với khoảng hiệu lực
[valid_from, valid_to). Trả lời được câu hỏi silver không trả lời được: tin tăng lương sau bao lâu, tin nào
đổi tiêu đề, lương lúc tin mới đăng là bao nhiêu.

Cách làm – "tính lại theo khóa bị ảnh hưởng" thay vì nối thêm dòng:
1. Lấy các tin (job_uid) có dòng bronze mới (ingest_ts > mốc 'joblens.bronze_watermark' trên bảng history).
2. Đọc LẠI MỌI lần crawl của các tin đó trong bronze (bronze bất biến – nguyên tắc immutability của Lambda),
   chuẩn hóa bằng đúng quy tắc của silver (normalize.py), sắp theo (crawl_ts, kafka_offset).
3. Gom các lần crawl liên tiếp có cùng attr_hash (hash các cột theo dõi) thành một phiên bản; valid_to = valid_from
   của phiên bản sau; changed_fields = các cột khác phiên bản trước.
4. So với các dòng hiện có của đúng các tin đó → chỉ giữ thay đổi thật: I (thêm), U (sửa), D (xóa – phiên bản
   thừa khi một bản crawl đến trễ làm gộp lại hai phiên bản).
5. Một lệnh MERGE INTO duy nhất áp I/U/D → một snapshot Iceberg, người đọc không thấy trạng thái dở dang.

Vì sao không nối thêm (append) phiên bản mới như SCD2 thường làm: bronze nhận bản crawl đến trễ / sai thứ tự
(replay, backfill VietJobs, loại nhiễu "đến sai thứ tự" trong thí nghiệm nhiễu). Nối thêm theo thứ tự đến sẽ sai
khoảng hiệu lực. Tính lại cả dòng thời gian của tin từ bronze thì kết quả chỉ phụ thuộc vào TẬP các lần crawl,
không phụ thuộc thứ tự đến; chạy lại bao nhiêu lần cũng cho cùng kết quả (idempotent).
Cái giá: mỗi lần chạy phải quét bronze để lấy lịch sử các tin bị ảnh hưởng (bronze phân vùng theo ngày nạp,
không theo tin) – chấp nhận được ở ~10^5 dòng; lớn hơn thì cần bucket bronze theo job_uid (ghi trong báo cáo).

Cột theo contract joblens/schemas/tables/silver/job_postings_history.yaml (0.1.0).

Chạy:  bash deploy/spark/submit.sh silver_job_history [--full]
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

CATALOG = "lake"
BRONZE = f"{CATALOG}.bronze.jobs_raw"
HISTORY = f"{CATALOG}.silver.job_postings_history"
WATERMARK_PROP = "joblens.bronze_watermark"

# Cột theo dõi: đổi một trong các cột này thì sinh phiên bản mới. Sửa mô tả/yêu cầu/phúc lợi thì không
# (văn bản dài hay sửa lặt vặt; đã có content_hash ở silver.job_postings).
TRACKED = [
    "title", "company_key", "salary_min_vnd", "salary_max_vnd", "salary_is_negotiable", "location_codes",
    "level_code", "experience_min_years", "experience_max_years", "employment_type_code", "deadline",
]  # fmt: skip

HISTORY_COLUMNS = [
    "job_uid", "version_no", "valid_from", "valid_to", "is_current", "change_type", "changed_fields", "source",
    *TRACKED, "attr_hash", "last_seen_ts", "crawl_count", "updated_ts",
]  # fmt: skip

DDL = f"""
CREATE TABLE IF NOT EXISTS {HISTORY} (
    job_uid STRING NOT NULL, version_no INT NOT NULL, valid_from TIMESTAMP NOT NULL, valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL, change_type STRING NOT NULL, changed_fields ARRAY<STRING> NOT NULL,
    source STRING NOT NULL, title STRING NOT NULL, company_key STRING,
    salary_min_vnd BIGINT COMMENT 'VND/tháng', salary_max_vnd BIGINT COMMENT 'VND/tháng',
    salary_is_negotiable BOOLEAN NOT NULL, location_codes ARRAY<STRING> NOT NULL, level_code STRING,
    experience_min_years DOUBLE COMMENT 'năm', experience_max_years DOUBLE COMMENT 'năm',
    employment_type_code STRING, deadline DATE, attr_hash STRING NOT NULL, last_seen_ts TIMESTAMP NOT NULL,
    crawl_count BIGINT NOT NULL, updated_ts TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (bucket(8, job_uid))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'comment' = 'Lịch sử phiên bản tin (SCD loại 2) – contract joblens silver.job_postings_history 0.1.0'
)
"""

_KEYS = ("job_uid", "version_no")
MERGE_SQL = f"""
MERGE INTO {HISTORY} t
USING history_changes s
ON t.job_uid = s.job_uid AND t.version_no = s.version_no
WHEN MATCHED AND s._op = 'D' THEN DELETE
WHEN MATCHED AND s._op = 'U' THEN UPDATE SET
    {", ".join(f"{c} = s.{c}" for c in HISTORY_COLUMNS if c not in _KEYS)}
WHEN NOT MATCHED AND s._op = 'I' THEN INSERT ({", ".join(HISTORY_COLUMNS)})
    VALUES ({", ".join(f"s.{c}" for c in HISTORY_COLUMNS)})
"""

log = logging.getLogger("silver_job_history")


def with_job_uid(bronze: DataFrame) -> DataFrame:
    return bronze.withColumn(
        "job_uid", F.sha1(F.concat_ws(":", "source", "source_job_id"))
    )  # = joblens_contracts.job_uid


def crawl_states(bronze: DataFrame, normalizer=None) -> DataFrame:
    """Mỗi lần crawl → trạng thái các cột theo dõi (đã chuẩn hóa như silver) + attr_hash.

    Bản trùng chính xác (cùng tin, cùng crawl_ts, cùng nội dung theo dõi) chỉ giữ một.
    """
    import silver_job_postings  # dùng chung UDF chuẩn hóa với silver – một nguồn quy tắc (ADR 0001 D2)

    n = (normalizer or silver_job_postings.normalize_udf)("payload_json")
    rows = with_job_uid(bronze).select(
        "job_uid", "source", "crawl_ts", "kafka_offset", n.alias("n")
    )
    cols = []
    for c in TRACKED:
        col = F.col(f"n.{c}")
        if c == "title":
            col = F.coalesce(col, F.lit(""))
        elif c == "salary_is_negotiable":
            col = F.coalesce(col, F.lit(False))
        elif c == "location_codes":  # thứ tự địa điểm trong tin không phải là thay đổi
            col = F.array_sort(F.coalesce(col, F.array().cast("array<string>")))
        cols.append(col.alias(c))
    states = rows.select("job_uid", "source", "crawl_ts", "kafka_offset", *cols)
    states = states.withColumn(
        "attr_hash", F.sha1(F.to_json(F.struct(*[F.col(c) for c in TRACKED])))
    )
    return states.dropDuplicates(["job_uid", "crawl_ts", "attr_hash"])


def build_history(states: DataFrame, now: datetime | None = None) -> DataFrame:
    """Trạng thái từng lần crawl → các phiên bản (SCD loại 2), đúng thứ tự cột HISTORY_COLUMNS."""
    now_col = F.lit(now).cast("timestamp") if now else F.current_timestamp()
    by_crawl = Window.partitionBy("job_uid").orderBy("crawl_ts", "kafka_offset")
    marked = states.withColumn(
        "_new",
        F.coalesce(F.lag("attr_hash").over(by_crawl) != F.col("attr_hash"), F.lit(True)).cast("int"),
    )  # fmt: skip
    marked = marked.withColumn("version_no", F.sum("_new").over(by_crawl).cast("int"))
    first = F.min_by(
        F.struct("source", *TRACKED, "attr_hash"),
        F.struct("crawl_ts", "kafka_offset"),
    )
    versions = marked.groupBy("job_uid", "version_no").agg(
        F.min("crawl_ts").alias("valid_from"),
        F.max("crawl_ts").alias("last_seen_ts"),
        F.count("*").alias("crawl_count"),
        first.alias("v"),
    )
    by_version = Window.partitionBy("job_uid").orderBy("version_no")
    versions = versions.select(
        "job_uid", "version_no", "valid_from", "last_seen_ts", "crawl_count", "v.*"
    ).withColumn("valid_to", F.lead("valid_from").over(by_version))
    changed = [
        F.when(
            ~F.col(c).eqNullSafe(F.lag(c).over(by_version)), F.lit(c)
        )  # null-safe: null → giá trị cũng là thay đổi
        for c in TRACKED
    ]
    versions = versions.withColumn(
        "changed_fields",
        F.when(F.col("version_no") == 1, F.array().cast("array<string>")).otherwise(
            F.filter(F.array(*changed), lambda x: x.isNotNull())
        ),
    )
    return versions.select(
        "job_uid",
        "version_no",
        "valid_from",
        "valid_to",
        F.col("valid_to").isNull().alias("is_current"),
        F.when(F.col("version_no") == 1, "new").otherwise("changed").alias("change_type"),
        "changed_fields",
        "source",
        *TRACKED,
        "attr_hash",
        "last_seen_ts",
        "crawl_count",
        now_col.alias("updated_ts"),
    )


_COMPARED = ["valid_from", "valid_to", "change_type", "changed_fields", "attr_hash", "last_seen_ts", "crawl_count"]  # fmt: skip


def merge_changes(new: DataFrame, current: DataFrame) -> DataFrame:
    """So lịch sử vừa tính (new) với dòng hiện có (current) CỦA CÙNG CÁC TIN → chỉ các dòng cần ghi, cột _op:
    I = phiên bản chưa có, U = phiên bản có nhưng khác, D = phiên bản thừa (không còn trong lịch sử mới).
    Dòng giống hệt thì bỏ – MERGE không ghi lại, không sinh file mới."""
    n, c = new.alias("n"), current.select(*_KEYS, *_COMPARED).alias("c")
    j = n.join(c, list(_KEYS), "full_outer")
    differs = F.lit(False)
    for col in _COMPARED:
        differs = differs | ~F.col(f"n.{col}").eqNullSafe(F.col(f"c.{col}"))
    op = (
        F.when(F.col("c.attr_hash").isNull(), "I")
        .when(F.col("n.attr_hash").isNull(), "D")
        .when(differs, "U")
    )
    return (
        j.withColumn("_op", op)
        .filter(F.col("_op").isNotNull())
        .select(
            *_KEYS,
            *[F.col(f"n.{col}").alias(col) for col in HISTORY_COLUMNS if col not in _KEYS],
            "_op",
        )
    )


def main(argv: list[str] | None = None) -> None:
    import bronze_jobs

    p = argparse.ArgumentParser(description="bronze.jobs_raw → silver.job_postings_history (SCD loại 2)")
    p.add_argument("--full", action="store_true", help="Bỏ qua mốc, tính lại lịch sử của mọi tin")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    spark: SparkSession = bronze_jobs.build_session("silver-job-history")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    spark.sql(DDL)
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {HISTORY}").collect()}
    mark = None if args.full else props.get(WATERMARK_PROP)

    bronze = with_job_uid(spark.table(BRONZE))
    fresh = bronze.filter(F.col("ingest_ts") > F.lit(mark).cast("timestamp")) if mark else bronze
    new_mark = fresh.agg(F.max("ingest_ts")).first()[0]
    if new_mark is None:
        log.info("không có dòng bronze mới sau mốc %s – lịch sử giữ nguyên", mark)
        return

    touched = fresh.select("job_uid").distinct().persist()
    n_touched = touched.count()
    crawls = bronze.join(touched, "job_uid", "left_semi").drop("job_uid")
    new = build_history(crawl_states(crawls))
    current = spark.table(HISTORY).join(touched, "job_uid", "left_semi")
    changes = merge_changes(new, current).persist()
    ops = {r["_op"]: r["n"] for r in changes.groupBy("_op").agg(F.count("*").alias("n")).collect()}
    if ops:
        changes.createOrReplaceTempView("history_changes")
        spark.sql(MERGE_SQL)
    spark.sql(f"ALTER TABLE {HISTORY} SET TBLPROPERTIES ('{WATERMARK_PROP}' = '{new_mark.isoformat()}')")
    log.info("tin bị ảnh hưởng %s (mốc bronze %s → %s); thêm %s, sửa %s, xóa %s phiên bản",
             n_touched, mark, new_mark, ops.get("I", 0), ops.get("U", 0), ops.get("D", 0))  # fmt: skip

    s = spark.sql(f"""
        SELECT count(*) AS versions, count(DISTINCT job_uid) AS jobs,
               sum(CAST(is_current AS INT)) AS current_rows,
               count(DISTINCT CASE WHEN version_no > 1 THEN job_uid END) AS jobs_changed
        FROM {HISTORY}""").first()  # fmt: skip
    log.info("silver.job_postings_history: %s phiên bản của %s tin (%s dòng hiện tại), %s tin từng thay đổi",
             s["versions"], s["jobs"], s["current_rows"], s["jobs_changed"])  # fmt: skip
    if s["current_rows"] != s["jobs"]:  # bất biến SCD2: mỗi tin đúng một phiên bản hiện tại
        raise RuntimeError(f"Sai bất biến: {s['current_rows']} dòng is_current cho {s['jobs']} tin")
    touched.unpersist()
    changes.unpersist()


if __name__ == "__main__":
    main()
