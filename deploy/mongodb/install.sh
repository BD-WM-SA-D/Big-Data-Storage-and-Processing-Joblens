#!/usr/bin/env bash
# Cài MongoDB 8.0 vào namespace "serving", tạo user ứng dụng "joblens" (readWrite trên database joblens),
# và Secret "joblens-mongo" (biến JOBLENS_MONGO_URL) cho Spark (compute) và Trino (serving).
# Chạy từ gốc repo:  bash deploy/mongodb/install.sh   (chạy lại nhiều lần vẫn an toàn)
# Sau đó chạy lại deploy/trino/install.sh để Trino có catalog "mongodb".
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"
NS=serving
rand() { python3 -c "import secrets,string,sys; a=string.ascii_letters+string.digits; print(''.join(secrets.choice(a) for _ in range(int(sys.argv[1]))))" "$1"; }
val() { kubectl get secret mongodb-auth -n "$NS" -o jsonpath="{.data.$1}" | base64 -d; }

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
if ! kubectl get secret mongodb-auth -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic mongodb-auth -n "$NS" \
    --from-literal=root-password="$(rand 32)" --from-literal=app-password="$(rand 32)"
fi

echo "==> MongoDB 8.0"
kubectl apply -f "${HERE}/mongodb.yaml"
kubectl rollout status statefulset/mongodb -n "$NS" --timeout=5m

echo "==> User ứng dụng 'joblens'"
# Lần khởi động đầu, image mongo chạy một mongod tạm (tạo user root) rồi tự khởi động lại – readiness có thể đạt
# trong pha tạm đó, nên thử lại vài lần thay vì lỗi ngay (gặp 2026-10-23: ECONNREFUSED 127.0.0.1:27017).
ok=""
for _ in $(seq 1 12); do
  if kubectl exec -n "$NS" mongodb-0 -- mongosh --quiet -u root -p "$(val root-password)" --authenticationDatabase admin --eval "
    const pw = '$(val app-password)';
    const a = db.getSiblingDB('admin');
    if (a.getUser('joblens')) { a.updateUser('joblens', {pwd: pw, roles: [{role: 'readWrite', db: 'joblens'}]}); print('   đã cập nhật'); }
    else { a.createUser({user: 'joblens', pwd: pw, roles: [{role: 'readWrite', db: 'joblens'}]}); print('   đã tạo'); }"; then
    ok=1; break
  fi
  echo "   MongoDB chưa nhận kết nối – thử lại sau 5 giây"; sleep 5
done
[ -n "$ok" ] || { echo "Không tạo được user sau 1 phút – xem: kubectl logs -n $NS mongodb-0"; exit 1; }

URL="mongodb://joblens:$(val app-password)@mongodb.serving.svc.cluster.local:27017/joblens?authSource=admin"
for ns in compute serving; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl create secret generic joblens-mongo -n "$ns" --from-literal=JOBLENS_MONGO_URL="$URL" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done
echo "Xong. Xem dữ liệu: kubectl exec -it -n $NS mongodb-0 -- mongosh -u root -p \"\$(kubectl get secret mongodb-auth -n $NS -o jsonpath='{.data.root-password}' | base64 -d)\" joblens --authenticationDatabase admin"
echo "Tiếp theo: bash deploy/trino/install.sh (thêm catalog mongodb)"
