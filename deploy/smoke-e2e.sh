#!/usr/bin/env bash
# Smoke test toàn tuyến ("walking skeleton", docs/project-plan/10 – P1):
#   fixtures job_posting.v1 → Kafka jobs.raw.topcv.v1 → Spark streaming (--once) → Iceberg bronze.jobs_raw → Trino
# Cần đã cài: deploy/kafka, deploy/storage, deploy/trino. Chạy từ gốc repo:  bash deploy/smoke-e2e.sh [../joblens]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
JOBLENS_DIR="${1:-${HERE}/../../joblens}"

echo "### 1/3 Gửi fixtures vào Kafka"
bash "${HERE}/kafka/smoke-test.sh" "$JOBLENS_DIR"

echo "### 2/3 Spark: Kafka → bronze.jobs_raw (xử lý hết rồi dừng)"
bash "${HERE}/spark/submit.sh" bronze_jobs --once

echo "### 3/3 Trino: đọc bronze.jobs_raw"
COORD=$(kubectl get pod -n serving -l app.kubernetes.io/component=coordinator -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n serving "$COORD" -- trino --output-format=ALIGNED --execute "
  SELECT source, count(*) AS so_dong, count(DISTINCT source_job_id) AS so_tin, max(ingest_ts) AS lan_nap_cuoi
  FROM iceberg.bronze.jobs_raw GROUP BY source"
echo "Đạt nếu thấy dòng source=synthetic (mỗi lần chạy smoke test gửi thêm 3 bản ghi)."
