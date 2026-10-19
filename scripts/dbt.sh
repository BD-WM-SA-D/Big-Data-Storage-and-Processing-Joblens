#!/usr/bin/env bash
# Chạy dbt (tầng gold) trên Trino trong cụm qua port-forward.
#   bash scripts/dbt.sh build        # seed + run + test (mặc định)
#   bash scripts/dbt.sh run -s mart_salary_role_region
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${DBT_TRINO_PORT:-18080}"

kubectl port-forward -n serving svc/trino "${PORT}:8080" >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 20); do curl -sf "http://localhost:${PORT}/v1/info" >/dev/null && break; sleep 1; done

cd "${ROOT}/dbt"
DBT_TRINO_PORT="$PORT" DBT_SEND_ANONYMOUS_USAGE_STATS=false \
  uv run --project "$ROOT" dbt "${@:-build}" --project-dir . --profiles-dir .
