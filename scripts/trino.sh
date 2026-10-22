#!/usr/bin/env bash
# Chạy truy vấn Trino trong cụm (CLI có sẵn trong pod coordinator).
#   bash scripts/trino.sh "SELECT count(*) FROM iceberg.bronze.jobs_raw"
#   bash scripts/trino.sh -f scripts/demo-queries.sql
#   TRINO_FORMAT=CSV_HEADER_UNQUOTED bash scripts/trino.sh "…"   # đầu ra cho script (benchmark)
set -euo pipefail
FMT="${TRINO_FORMAT:-ALIGNED}"
COORD=$(kubectl get pod -n serving -l app.kubernetes.io/component=coordinator -o jsonpath='{.items[0].metadata.name}')
if [ "${1:-}" = "-f" ]; then
  kubectl exec -i -n serving "$COORD" -- trino --output-format="$FMT" < "$2"
else
  kubectl exec -n serving "$COORD" -- trino --output-format="$FMT" --execute "$1"
fi
