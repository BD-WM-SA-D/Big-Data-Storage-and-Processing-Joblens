#!/usr/bin/env bash
# Đo số byte Trino thật sự đọc cho hai truy vấn mẫu của layout_iceberg (docs/bien-doi-du-lieu.md, T6).
#   bash scripts/bench-layout.sh            # chạy TRƯỚC: EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh layout_iceberg
#   bash scripts/bench-layout.sh            # chạy lại SAU job, so hai bảng
# Dùng EXPLAIN ANALYZE: Trino chạy thật truy vấn và in "Physical input" của bước đọc bảng (byte đọc từ S3).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
v() { TRINO_FORMAT=CSV_UNQUOTED bash scripts/trino.sh "$1" | tail -1; }

ID=$(v "SELECT source_job_id FROM iceberg.bronze.jobs_raw WHERE source = 'vietjobs' ORDER BY to_hex(sha1(to_utf8(source_job_id))) LIMIT 1")
Q1="SELECT count(*), max(crawl_ts) FROM iceberg.bronze.jobs_raw WHERE source = 'vietjobs' AND source_job_id = '${ID}'"
Q2="SELECT count(*) FROM iceberg.silver.job_postings_history WHERE valid_from >= current_timestamp - INTERVAL '7' DAY"

echo "| Truy vấn | Physical input | Dòng đọc |"
echo "|---|---|---|"
for q in "$Q1" "$Q2"; do
  out=$(TRINO_FORMAT=CSV_UNQUOTED bash scripts/trino.sh "EXPLAIN ANALYZE $q" || true)
  phys=$(grep -o 'Physical input: [0-9.]*[kMG]*B' <<<"$out" | head -1 | sed 's/Physical input: //')
  rows=$(grep -o 'Input: [0-9,.]*[kM]* rows' <<<"$out" | head -1 | sed 's/Input: //')
  printf '| `%s` | %s | %s |\n' "$q" "${phys:-?}" "${rows:-?}"
done
echo "(Chép bảng vào benchmarks/layout_iceberg.md – ghi rõ trước/sau, thời điểm, commit; không bịa số.)"
