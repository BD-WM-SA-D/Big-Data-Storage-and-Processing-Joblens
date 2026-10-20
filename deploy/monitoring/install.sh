#!/usr/bin/env bash
# Cài giao diện quan sát cho buổi demo vào namespace "monitoring":
#   Grafana 13.2.4 + plugin Trino – dashboard dashboards/joblens-demo.json (đọc bảng Iceberg qua Trino)
#   Kafka UI v1.5.0 (chỉ đọc)    – topic, message, lag của consumer
# Cần chạy trước: deploy/kafka, deploy/trino. Chạy từ gốc repo:  bash deploy/monitoring/install.sh
# Sửa dashboard: sửa trên Grafana (đăng nhập admin) → Export JSON → ghi đè dashboards/joblens-demo.json → chạy lại.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=monitoring
rand() { python3 -c "import secrets,string,sys; a=string.ascii_letters+string.digits; print(''.join(secrets.choice(a) for _ in range(int(sys.argv[1]))))" "$1"; }

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
if ! kubectl get secret grafana-admin -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic grafana-admin -n "$NS" --from-literal=password="$(rand 16)"
fi
kubectl create configmap grafana-dashboards -n "$NS" --from-file="${HERE}/dashboards/" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl apply -f "${HERE}/grafana.yaml" -f "${HERE}/kafka-ui.yaml"
# ConfigMap gắn bằng subPath không tự cập nhật → khởi động lại khi chạy lại script
kubectl rollout restart deployment/grafana -n "$NS" >/dev/null
echo "==> Chờ Grafana (lần đầu tải plugin Trino, ~1–2 phút) và Kafka UI"
kubectl rollout status deployment/grafana -n "$NS" --timeout=10m
kubectl rollout status deployment/kafka-ui -n "$NS" --timeout=10m
kubectl get pods -n "$NS"
echo
echo "Mở bằng: bash scripts/demo-ui.sh  – Grafana http://localhost:3000 (không cần đăng nhập để xem;"
echo "admin / $(kubectl get secret grafana-admin -n "$NS" -o jsonpath='{.data.password}' | base64 -d)), Kafka UI http://localhost:8081"
