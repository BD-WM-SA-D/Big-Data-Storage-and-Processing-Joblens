#!/usr/bin/env bash
# Chuyển "chế độ" cụm trong buổi demo để vừa máy 16 GB RAM (WSL ~12 GB): không chạy streaming và Airflow cùng lúc.
#   bash scripts/demo-mode.sh streaming   # tạm dừng DAG, đợi task đang chạy xong, rồi tắt scheduler + dag-processor
#                                         # (giao diện Airflow vẫn mở được). Không giết task giữa chừng.
#   bash scripts/demo-mode.sh airflow     # dừng job streaming, bật lại Airflow và bỏ tạm dừng DAG
#   bash scripts/demo-mode.sh status      # RAM từng node và 12 pod dùng nhiều RAM nhất
set -euo pipefail
# shellcheck source=deploy/lib.sh
. "$(cd "$(dirname "$0")/.." && pwd)/deploy/lib.sh"
NS_AF=orchestration

status() {
  echo "==> RAM theo node"
  kubectl top nodes 2>/dev/null || echo "   (metrics-server chưa sẵn sàng – thử lại sau 1 phút)"
  echo "==> Pod dùng nhiều RAM nhất"
  kubectl top pods -A --sort-by=memory 2>/dev/null | head -13 || true
  echo "==> Bộ nhớ WSL"
  free -h | head -2
}

af() {  # af ARGS… – chạy lệnh airflow trong pod scheduler (khi scheduler đang chạy)
  kubectl exec -n "$NS_AF" airflow-scheduler-0 -c scheduler -- airflow "$@" >/dev/null 2>&1
}

wait_tasks_done() {  # đợi các pod task của DAG trong compute kết thúc (tối đa 20 phút)
  local n
  for _ in $(seq 1 120); do
    n=$(kubectl get pods -n compute -l kubernetes_pod_operator=True --field-selector=status.phase=Running \
          --no-headers 2>/dev/null | wc -l)
    [ "$n" -eq 0 ] && return 0
    echo "   còn ${n} task Airflow đang chạy – đợi xong (Ctrl+C để bỏ qua và tắt luôn)"
    sleep 10
  done
  echo "   quá 20 phút, vẫn tắt scheduler (task đang chạy sẽ bị đánh dấu lỗi)"
}

airflow_scale() {  # airflow_scale 0|1
  kubectl scale statefulset/airflow-scheduler -n "$NS_AF" --replicas="$1" >/dev/null 2>&1 \
    || kubectl scale deployment/airflow-scheduler -n "$NS_AF" --replicas="$1" >/dev/null
  kubectl scale deployment/airflow-dag-processor -n "$NS_AF" --replicas="$1" >/dev/null
}

case "${1:-status}" in
  streaming)
    echo "==> Tạm dừng DAG joblens_pipeline (không tạo lượt chạy mới)"
    af dags pause joblens_pipeline || echo "   (scheduler đang tắt – bỏ qua)"
    wait_tasks_done
    echo "==> Tắt Airflow scheduler + dag-processor (giải phóng ~1 GB)"
    airflow_scale 0
    echo "Giờ chạy: bash deploy/spark/submit.sh bronze_jobs --trigger \"10 seconds\"  (xem docs/demo.md mục 3.1)"
    ;;
  airflow)
    echo "==> Dừng các job streaming (bronze_jobs, stream_province_windows)"
    stop_spark_job bronze-jobs
    stop_spark_job stream-province-windows
    echo "==> Bật lại Airflow"
    airflow_scale 1
    kubectl rollout status statefulset/airflow-scheduler -n "$NS_AF" --timeout=5m 2>/dev/null \
      || kubectl rollout status deployment/airflow-scheduler -n "$NS_AF" --timeout=5m
    kubectl rollout status deployment/airflow-dag-processor -n "$NS_AF" --timeout=5m
    af dags unpause joblens_pipeline && echo "==> Đã bỏ tạm dừng DAG joblens_pipeline"
    echo "Giờ mở Airflow → joblens_pipeline → Trigger (docs/demo.md mục 3.2)"
    ;;
  status) status ;;
  *) echo "Dùng: $0 streaming|airflow|status"; exit 1 ;;
esac
