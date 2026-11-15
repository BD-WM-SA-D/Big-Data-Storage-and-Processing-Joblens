#!/usr/bin/env bash
# Đo hai mart gold tăng dần: dựng lại toàn bộ (--full-refresh) so với chạy tăng dần (docs/bien-doi-du-lieu.md, T2).
#   bash scripts/bench-dbt-incremental.sh            # full-refresh rồi tăng dần ngay (không có dữ liệu mới)
#   bash scripts/bench-dbt-incremental.sh --inc-only # chỉ chạy tăng dần (dùng sau khi có dữ liệu mới, ví dụ
#                                                    #   sau recrawl_sim + silver_job_postings + silver_job_history)
# In bảng markdown (thời gian mỗi model theo target/run_results.json của dbt, số dòng ghi, số dòng của mart)
# để chép vào benchmarks/incremental_gold.md. Chạy khi DAG đang tạm dừng (bash scripts/demo-mode.sh pause) để
# task dbt_build_gold của Airflow không chạy chen.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MODELS="mart_hiring_weekly mart_job_changes_weekly"

summary() {  # summary <nhãn> – đọc dbt/target/run_results.json
  uv run --project "$ROOT" python - "$1" <<'PY'
import json, sys
label = sys.argv[1]
r = json.load(open("dbt/target/run_results.json", encoding="utf-8"))
for x in r["results"]:
    name = x["unique_id"].split(".")[-1]
    rows = (x.get("adapter_response") or {}).get("rows_affected", "?")
    print(f"| {label} | {name} | {x['status']} | {x['execution_time']:.1f} | {rows} |")
PY
}
count() {
  for m in $MODELS; do
    printf '%s: %s dòng\n' "$m" "$(TRINO_FORMAT=CSV_UNQUOTED bash scripts/trino.sh "SELECT count(*) FROM iceberg.gold.$m" | tail -1)"
  done
}

echo "| Lần chạy | Model | Trạng thái | Thời gian (s) | Dòng ghi |"
echo "|---|---|---|---|---|"
if [ "${1:-}" != "--inc-only" ]; then
  # shellcheck disable=SC2086
  bash scripts/dbt.sh run -s $MODELS --full-refresh >/tmp/joblens-dbt-full.log 2>&1 || { cat /tmp/joblens-dbt-full.log; exit 1; }
  summary "full-refresh"
fi
# shellcheck disable=SC2086
bash scripts/dbt.sh run -s $MODELS >/tmp/joblens-dbt-inc.log 2>&1 || { cat /tmp/joblens-dbt-inc.log; exit 1; }
summary "tăng dần"
echo
count
echo "Log dbt: /tmp/joblens-dbt-full.log, /tmp/joblens-dbt-inc.log – không commit log, chỉ chép bảng số liệu."
