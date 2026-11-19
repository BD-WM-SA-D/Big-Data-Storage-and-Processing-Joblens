#!/usr/bin/env bash
# Chạy dbt (tầng gold) trên Trino trong cụm qua port-forward.
#   bash scripts/dbt.sh build        # seed + run + test (mặc định)
#   bash scripts/dbt.sh run -s mart_salary_role_region
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${DBT_TRINO_PORT:-18080}"

# shellcheck source=../deploy/lib.sh
. "${ROOT}/deploy/lib.sh"
# Profile lite (máy 8 GB) chạy 1 luồng để không dồn hai truy vấn nặng vào Trino một node.
THREADS="${DBT_THREADS:-$([ "$PROFILE" = lite ] && echo 1 || echo 2)}"
[ "$#" -eq 0 ] && set -- build

LOG="$(mktemp)"
PFPID_FILE="$(mktemp)"

# Port-forward tự kết nối lại nếu pod Trino bị thay/khởi động lại giữa chừng.
pkill -f "kubectl port-forward -n serving svc/trino ${PORT}:8080" 2>/dev/null || true
( while true; do
    kubectl port-forward -n serving svc/trino "${PORT}:8080" >/dev/null 2>&1 &
    echo $! > "$PFPID_FILE"
    wait $! || true
    sleep 1
  done ) &
PF_LOOP=$!

cleanup() {
  kill "$PF_LOOP" 2>/dev/null || true
  [ -s "$PFPID_FILE" ] && kill "$(cat "$PFPID_FILE")" 2>/dev/null || true
  rm -f "$PFPID_FILE" "$LOG"
}
trap cleanup EXIT

wait_trino() {
  local i
  for i in $(seq 1 180); do
    curl -sf "http://localhost:${PORT}/v1/info" >/dev/null && return 0
    sleep 1
  done
  return 1
}
wait_trino || { echo "Trino chưa sẵn sàng sau 180 giây" >&2; exit 1; }

run_dbt() {
  cd "${ROOT}/dbt"
  DBT_TRINO_PORT="$PORT" DBT_THREADS="$THREADS" DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    uv run --project "$ROOT" dbt "$@" --project-dir . --profiles-dir .
}

if ! run_dbt "$@" 2>&1 | tee "$LOG"; then
  if grep -qE "Connection refused|Max retries exceeded|Connection reset|failed to (fetch|execute)" "$LOG"; then
    echo "dbt mất kết nối Trino (pod vừa khởi động lại?) – chờ Ready rồi chạy lại" >&2
    wait_trino || { echo "Trino không trở lại sau 180 giây" >&2; exit 1; }
    run_dbt "$@"
  else
    exit 1
  fi
fi
