#!/usr/bin/env bash
# Cài Trino 483 vào namespace "serving", catalog "iceberg" → Lakekeeper warehouse "joblens".
# Cần chạy deploy/storage/install.sh trước. Chạy từ gốc repo:  bash deploy/trino/install.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"

TRINO_CHART_VERSION=1.44.0   # ứng dụng Trino 483 (joblens/versions.md)
NS=serving

load_env
s3_secret "$NS"

helm repo add trino https://trinodb.github.io/charts >/dev/null 2>&1 || true
helm repo update trino >/dev/null

echo "==> Trino (chart ${TRINO_CHART_VERSION})"
# catalog mongodb chỉ bật khi đã cài deploy/mongodb (cần Secret joblens-mongo)
mongo_values() { if kubectl get secret joblens-mongo -n "$NS" >/dev/null 2>&1; then echo "-f ${HERE}/values-mongodb.yaml"; fi; }
# shellcheck disable=SC2046  # profile_values cố ý tách thành hai đối số (-f FILE)
helm upgrade --install trino trino/trino --version "$TRINO_CHART_VERSION" \
  -n "$NS" -f "${HERE}/values.yaml" $(profile_values "$HERE") $(mongo_values) --wait --timeout 10m
# Secret có thể đổi mà values không đổi → khởi động lại để pod đọc khóa mới
kubectl rollout restart deployment -n "$NS" -l app.kubernetes.io/name=trino >/dev/null
kubectl rollout status deployment -n "$NS" -l app.kubernetes.io/name=trino --timeout=5m

echo "==> Kiểm tra: SHOW SCHEMAS FROM iceberg"
COORD=$(kubectl get pod -n "$NS" -l app.kubernetes.io/component=coordinator -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n "$NS" "$COORD" -- trino --execute "SHOW SCHEMAS FROM iceberg"
echo "Mở trình duyệt Trino UI: kubectl port-forward -n $NS svc/trino 8080:8080  →  http://localhost:8080 (user bất kỳ)"
