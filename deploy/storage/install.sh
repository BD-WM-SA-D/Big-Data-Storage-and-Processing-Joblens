#!/usr/bin/env bash
# Cài tầng lưu trữ vào namespace "lakehouse": SeaweedFS (S3) + Lakekeeper (Iceberg REST catalog) + Postgres,
# rồi bootstrap Lakekeeper và tạo warehouse "joblens" trỏ vào s3://joblens/warehouse.
# Chạy từ gốc repo:  bash deploy/storage/install.sh   (chạy lại nhiều lần vẫn an toàn)
#
# Bí mật: khóa S3 lấy từ .env (JOBLENS_S3_ACCESS_KEY_ID / JOBLENS_S3_SECRET_ACCESS_KEY); nếu trống thì
# script sinh ngẫu nhiên và ghi vào .env (file đã gitignore). Mật khẩu Postgres và khóa mã hóa chỉ nằm
# trong Secret của cụm, sinh một lần.
set -euo pipefail

SEAWEEDFS_CHART_VERSION=4.48.0
LAKEKEEPER_CHART_VERSION=0.12.0
NS=lakehouse
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"

rand() { python3 -c "import secrets,string,sys; a=string.ascii_letters+string.digits; print(''.join(secrets.choice(a) for _ in range(int(sys.argv[1]))))" "$1"; }
set_env() {  # set_env KEY VALUE – thay hoặc thêm dòng KEY=VALUE trong .env
  if grep -q "^$1=" "$ENV_FILE"; then sed -i "s|^$1=.*|$1=$2|" "$ENV_FILE"; else echo "$1=$2" >> "$ENV_FILE"; fi
}

load_env
if [ -z "${JOBLENS_S3_ACCESS_KEY_ID:-}" ] || [ -z "${JOBLENS_S3_SECRET_ACCESS_KEY:-}" ]; then
  echo "==> Sinh khóa S3 và ghi vào .env"
  JOBLENS_S3_ACCESS_KEY_ID="joblens$(rand 12)"
  JOBLENS_S3_SECRET_ACCESS_KEY="$(rand 40)"
  set_env JOBLENS_S3_ACCESS_KEY_ID "$JOBLENS_S3_ACCESS_KEY_ID"
  set_env JOBLENS_S3_SECRET_ACCESS_KEY "$JOBLENS_S3_SECRET_ACCESS_KEY"
fi

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Secret"
S3_CONFIG=$(printf '{"identities":[{"name":"joblens","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List","Tagging"]}]}' \
  "$JOBLENS_S3_ACCESS_KEY_ID" "$JOBLENS_S3_SECRET_ACCESS_KEY")
kubectl create secret generic seaweedfs-s3-config -n "$NS" \
  --from-literal=seaweedfs_s3_config="$S3_CONFIG" --dry-run=client -o yaml | kubectl apply -f -
if ! kubectl get secret lakekeeper-pg -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic lakekeeper-pg -n "$NS" \
    --from-literal=superuser=postgres --from-literal=superuser-password="$(rand 32)" \
    --from-literal=database=lakekeeper --from-literal=user=lakekeeper --from-literal=password="$(rand 32)"
fi
if ! kubectl get secret lakekeeper-encryption -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic lakekeeper-encryption -n "$NS" --from-literal=encryptionKey="$(rand 48)"
fi

echo "==> Helm repo"
helm repo add seaweedfs https://seaweedfs.github.io/seaweedfs/helm >/dev/null 2>&1 || true
helm repo add lakekeeper https://lakekeeper.github.io/lakekeeper-charts/ >/dev/null 2>&1 || true
helm repo update seaweedfs lakekeeper >/dev/null

echo "==> SeaweedFS ${SEAWEEDFS_CHART_VERSION}"
helm upgrade --install seaweedfs seaweedfs/seaweedfs --version "$SEAWEEDFS_CHART_VERSION" \
  -n "$NS" -f "${HERE}/seaweedfs-values.yaml" --wait --timeout 10m

echo "==> Lakekeeper ${LAKEKEEPER_CHART_VERSION}"
helm upgrade --install lakekeeper lakekeeper/lakekeeper --version "$LAKEKEEPER_CHART_VERSION" \
  -n "$NS" -f "${HERE}/lakekeeper-values.yaml" --wait --timeout 10m

echo "==> Bootstrap Lakekeeper và tạo warehouse 'joblens'"
kubectl port-forward -n "$NS" svc/lakekeeper 8181:8181 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -sf http://localhost:8181/health >/dev/null && break; sleep 1; done
MGMT=http://localhost:8181/management/v1
jget() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
if [ "$(curl -sf "$MGMT/info" | jget 'd["bootstrapped"]')" != "True" ]; then
  curl -sf -X POST "$MGMT/bootstrap" -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}'
  echo "   đã bootstrap"
fi
if curl -sf "$MGMT/warehouse" | jget '"joblens" in [w["name"] for w in d["warehouses"]]' | grep -q True; then
  echo "   warehouse 'joblens' đã có"
else
  python3 -c 'import os,sys; print(os.path.expandvars(open(sys.argv[1]).read()))' "${HERE}/warehouse.json.tmpl" \
    | curl -sf -X POST "$MGMT/warehouse" -H 'Content-Type: application/json' --data-binary @- >/dev/null \
    && echo "   đã tạo warehouse 'joblens'"
fi

echo "==> Kiểm tra"
kubectl get pods -n "$NS"
curl -sf "$MGMT/warehouse" | jget '"Warehouse: " + ", ".join(w["name"] for w in d["warehouses"])'
echo "Catalog URI trong cụm: http://lakekeeper.${NS}.svc.cluster.local:8181/catalog  (warehouse: joblens)"
