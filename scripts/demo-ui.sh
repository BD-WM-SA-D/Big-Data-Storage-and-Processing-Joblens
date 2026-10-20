#!/usr/bin/env bash
# Mở tất cả giao diện web của nền tảng cho buổi demo (port-forward từ cụm k3d ra localhost).
# Từ WSL, trình duyệt Windows mở được thẳng http://localhost:<cổng>.
#   bash scripts/demo-ui.sh          # Grafana, Kafka UI, Airflow, Trino, Lakekeeper, SeaweedFS – giữ terminal mở
#   bash scripts/demo-ui.sh spark    # Spark UI của driver mới nhất (job streaming hoặc task Airflow), cổng 4040
# Ctrl+C để đóng mọi port-forward. Dịch vụ nào chưa cài thì bỏ qua.
set -uo pipefail

pids=()
cleanup() { trap - INT TERM EXIT; kill 0 2>/dev/null; exit 0; }  # cả nhóm tiến trình: vòng lặp + kubectl
trap cleanup INT TERM EXIT

forward() {  # forward NS SVC LOCAL REMOTE LABEL PATH
  if kubectl get svc "$2" -n "$1" >/dev/null 2>&1; then
    # tự nối lại nếu pod khởi động lại (port-forward thoát khi pod mất)
    ( while true; do kubectl port-forward -n "$1" "svc/$2" "$3:$4" >/dev/null 2>&1; sleep 2; done ) &
    pids+=($!)
    printf '  %-12s http://localhost:%s%s\n' "$5" "$3" "$6"
  else
    printf '  %-12s (chưa cài: %s/%s)\n' "$5" "$1" "$2"
  fi
}

if [ "${1:-}" = "spark" ]; then
  echo "Đợi pod Spark driver (chạy job streaming hoặc trigger DAG Airflow)…"
  last=""
  while true; do
    pod=$(kubectl get pod -n compute -l spark-role=driver --field-selector=status.phase=Running \
          --sort-by=.metadata.creationTimestamp -o name 2>/dev/null | tail -1)
    if [ -n "$pod" ] && [ "$pod" != "$last" ]; then
      kill "${pids[@]}" 2>/dev/null; pids=()
      kubectl port-forward -n compute "$pod" 4040:4040 >/dev/null 2>&1 &
      pids+=($!); last="$pod"
      echo "  Spark UI     http://localhost:4040   ($pod – tab Structured Streaming khi chạy bronze_jobs)"
    fi
    sleep 3
  done
fi

echo "Giao diện demo (Ctrl+C để đóng):"
forward monitoring grafana            3000 3000 "Grafana"    "/d/joblens-demo"
forward monitoring kafka-ui           8081 8080 "Kafka UI"   "/ui/clusters/joblens/all-topics"
forward orchestration airflow-api-server 8082 8080 "Airflow" "/dags/joblens_pipeline"
forward serving    trino              8080 8080 "Trino"      "/ui/"
forward lakehouse  lakekeeper         8181 8181 "Lakekeeper" "/ui/"
forward lakehouse  seaweedfs-all-in-one 8888 8888 "SeaweedFS" "/buckets/joblens/warehouse/"
echo
echo "Mật khẩu: Grafana admin = kubectl get secret grafana-admin -n monitoring -o jsonpath='{.data.password}' | base64 -d"
echo "          Airflow admin = kubectl get secret airflow-admin -n orchestration -o jsonpath='{.data.password}' | base64 -d"
echo "          Trino: nhập user bất kỳ (dev không bật xác thực)"
wait
