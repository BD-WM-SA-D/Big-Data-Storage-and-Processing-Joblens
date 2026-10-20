#!/usr/bin/env bash
# Chuyển "chế độ" cụm trong buổi demo để vừa máy 16 GB RAM (WSL ~12 GB): không chạy streaming và Airflow cùng lúc.
#   bash scripts/demo-mode.sh streaming   # tạm tắt scheduler + dag-processor của Airflow (giao diện Airflow vẫn mở được)
#   bash scripts/demo-mode.sh airflow     # dừng job streaming bronze_jobs, bật lại Airflow để trigger DAG
#   bash scripts/demo-mode.sh status      # RAM từng node và 12 pod dùng nhiều RAM nhất
set -euo pipefail
NS_AF=orchestration

status() {
  echo "==> RAM theo node"
  kubectl top nodes 2>/dev/null || echo "   (metrics-server chưa sẵn sàng – thử lại sau 1 phút)"
  echo "==> Pod dùng nhiều RAM nhất"
  kubectl top pods -A --sort-by=memory 2>/dev/null | head -13 || true
  echo "==> Bộ nhớ WSL"
  free -h | head -2
}

airflow_scale() {  # airflow_scale 0|1
  kubectl scale statefulset/airflow-scheduler -n "$NS_AF" --replicas="$1" >/dev/null 2>&1 \
    || kubectl scale deployment/airflow-scheduler -n "$NS_AF" --replicas="$1" >/dev/null
  kubectl scale deployment/airflow-dag-processor -n "$NS_AF" --replicas="$1" >/dev/null
}

case "${1:-status}" in
  streaming)
    echo "==> Tạm tắt Airflow scheduler + dag-processor (giải phóng ~1 GB)"
    airflow_scale 0
    echo "Giờ chạy: bash deploy/spark/submit.sh bronze_jobs --trigger \"10 seconds\"  (xem docs/demo.md mục 3.1)"
    ;;
  airflow)
    echo "==> Dừng job streaming (pod driver bronze_jobs và executor của nó)"
    for app in $(kubectl get pod -n compute -l spark-role=driver -o jsonpath='{range .items[*]}{.metadata.labels.spark-app-selector}{"\n"}{end}' 2>/dev/null); do
      name=$(kubectl get pod -n compute -l "spark-app-selector=${app},spark-role=driver" -o jsonpath='{.items[0].metadata.name}')
      case "$name" in bronze-jobs-*) kubectl delete pod -n compute -l "spark-app-selector=${app}" --wait=false >/dev/null; echo "   đã dừng $name";; esac
    done
    echo "==> Bật lại Airflow"
    airflow_scale 1
    kubectl rollout status statefulset/airflow-scheduler -n "$NS_AF" --timeout=5m 2>/dev/null \
      || kubectl rollout status deployment/airflow-scheduler -n "$NS_AF" --timeout=5m
    kubectl rollout status deployment/airflow-dag-processor -n "$NS_AF" --timeout=5m
    echo "Giờ mở Airflow → joblens_pipeline → Trigger (docs/demo.md mục 3.2)"
    ;;
  status) status ;;
  *) echo "Dùng: $0 streaming|airflow|status"; exit 1 ;;
esac
