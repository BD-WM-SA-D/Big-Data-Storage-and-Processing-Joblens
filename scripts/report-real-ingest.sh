#!/usr/bin/env bash
# Đối soát dữ liệu crawl thật sau khi nạp (thí nghiệm F – docs/thi-nghiem-du-lieu-that.md mục 3):
# Kafka (offset) ↔ bronze ↔ bronze.ingest_errors ↔ silver ↔ history ↔ bảng của WM ↔ gold, chỉ nguồn crawl
# (topcv, itviec). In bảng markdown; chép số vào docs, không commit đầu ra.
#   bash scripts/report-real-ingest.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
q() { echo; echo "### $1"; echo; TRINO_FORMAT=MARKDOWN bash scripts/trino.sh "$2" 2>/dev/null || bash scripts/trino.sh "$2"; }
CRAWL="('topcv', 'itviec')"

echo "# Đối soát dữ liệu crawl thật – $(date '+%Y-%m-%d %H:%M:%S')"
echo
echo "### 1. Kafka – tổng offset cuối (số message đã vào topic, kể cả đã hết hạn lưu)"
echo
echo "| Topic | Message |"
echo "|---|---|"
for t in jobs.raw.topcv.v1 jobs.raw.itviec.v1 reviews.raw.itviec.v1 dlq.parse_errors.v1; do
  n=$(kubectl exec -n ingest joblens-dual-role-0 -c kafka -- /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9092 --topic "$t" --time -1 2>/dev/null | awk -F: '{s += $3} END {print s + 0}')
  echo "| $t | $n |"
done

q "2. Bronze – dòng, tin khác nhau, số đợt crawl" "
SELECT source, count(*) AS bronze_rows, count(DISTINCT source_job_id) AS jobs,
       count(DISTINCT date(crawl_ts)) AS crawl_days, min(crawl_ts) AS first_crawl, max(crawl_ts) AS last_crawl
FROM iceberg.bronze.jobs_raw WHERE source IN $CRAWL GROUP BY 1
UNION ALL
SELECT 'review:' || source, count(*), count(DISTINCT source_review_id), count(DISTINCT date(crawl_ts)),
       min(crawl_ts), max(crawl_ts)
FROM iceberg.bronze.reviews_raw WHERE source <> 'synthetic' GROUP BY 1 ORDER BY 1"

q "3. Cách ly khi nạp bronze (message hỏng) theo topic và lý do" "
SELECT kafka_topic, reason, count(*) AS rows FROM iceberg.bronze.ingest_errors
WHERE kafka_topic LIKE 'jobs.raw.topcv%' OR kafka_topic LIKE 'jobs.raw.itviec%' OR kafka_topic LIKE 'reviews.raw.%'
GROUP BY 1, 2 ORDER BY 1, 3 DESC"

q "4. Silver – độ phủ chuẩn hóa trên dữ liệu thật (so với VietJobs)" "
SELECT source, count(*) AS jobs, count_if(is_active) AS active,
       round(100e0 * count_if(salary_min_vnd IS NOT NULL OR salary_max_vnd IS NOT NULL) / count(*), 1) AS pct_salary,
       round(100e0 * count_if(salary_is_negotiable) / count(*), 1) AS pct_negotiable,
       round(100e0 * count_if(cardinality(location_codes) > 0) / count(*), 1) AS pct_location,
       round(100e0 * count_if(level_code IS NOT NULL) / count(*), 1) AS pct_level,
       round(100e0 * count_if(experience_min_years IS NOT NULL) / count(*), 1) AS pct_experience,
       round(100e0 * count_if(posted_date IS NOT NULL) / count(*), 1) AS pct_posted_date
FROM iceberg.silver.job_postings WHERE source IN ('topcv', 'itviec', 'vietjobs') GROUP BY 1 ORDER BY 1"

q "5. Lịch sử tin (SCD loại 2) từ các đợt crawl thật" "
SELECT source, count(*) AS versions, count(DISTINCT job_uid) AS jobs,
       count(DISTINCT CASE WHEN version_no > 1 THEN job_uid END) AS jobs_changed,
       round(100e0 * count(DISTINCT CASE WHEN version_no > 1 THEN job_uid END) / count(DISTINCT job_uid), 1) AS pct_changed
FROM iceberg.silver.job_postings_history WHERE source IN $CRAWL GROUP BY 1 ORDER BY 1"
q "5b. Trường thay đổi giữa các đợt crawl thật" "
SELECT f AS changed_field, count(*) AS versions
FROM iceberg.silver.job_postings_history CROSS JOIN UNNEST(changed_fields) AS t (f)
WHERE source IN $CRAWL GROUP BY 1 ORDER BY 2 DESC"

q "6. Kết quả mô hình Web Mining trên tin crawl" "
SELECT 'job_skills' AS bang, count(*) AS rows, count(DISTINCT s.job_uid) AS jobs,
       round(1e0 * count(*) / nullif(count(DISTINCT s.job_uid), 0), 2) AS per_job, max(s.extractor_version) AS version
FROM iceberg.silver.job_skills s JOIN iceberg.silver.job_postings p ON p.job_uid = s.job_uid WHERE p.source IN $CRAWL
UNION ALL
SELECT 'job_dedup: nhóm > 1 tin', count(*), sum(n), NULL, max(v) FROM (
  SELECT d.dup_group_id, count(*) AS n, max(d.dedup_version) AS v FROM iceberg.silver.job_dedup d
  JOIN iceberg.silver.job_postings p ON p.job_uid = d.job_uid WHERE p.source IN $CRAWL
  GROUP BY 1 HAVING count(*) > 1)
UNION ALL
SELECT 'job_dedup: nhóm khác trang', count(*), NULL, NULL, NULL FROM (
  SELECT d.dup_group_id FROM iceberg.silver.job_dedup d JOIN iceberg.silver.job_postings p ON p.job_uid = d.job_uid
  WHERE p.source IN $CRAWL GROUP BY 1 HAVING count(DISTINCT p.source) > 1)
UNION ALL
SELECT 'review_aspects', count(*), count(DISTINCT review_uid), NULL, max(model_version)
FROM iceberg.silver.review_aspects"

q "7. Gold đọc kết quả mô hình" "
SELECT 'mart_skill_demand_monthly' AS bang, count(*) AS rows FROM iceberg.gold.mart_skill_demand_monthly
UNION ALL SELECT 'dim_company (có review)', count_if(review_count > 0) FROM iceberg.gold.dim_company
UNION ALL SELECT 'mart_company_aspect_sentiment', count(*) FROM iceberg.gold.mart_company_aspect_sentiment"
q "7b. 10 kỹ năng được yêu cầu nhiều nhất, tháng gần nhất (đếm theo nhóm tin trùng)" "
SELECT month, skill_label, job_count, round(100 * share, 1) AS pct_jobs, round(mom_change, 1) AS mom_pp
FROM iceberg.gold.mart_skill_demand_monthly
WHERE category_source = 'ALL' AND month = (SELECT max(month) FROM iceberg.gold.mart_skill_demand_monthly)
ORDER BY job_count DESC LIMIT 10"
