#!/usr/bin/env bash
# Cài công cụ thao tác dữ liệu trên trình duyệt (không cần terminal) vào namespace "serving":
#   SQLPad 7.5.7      – soạn và chạy SQL qua Trino: INSERT/UPDATE/DELETE trên Iceberg, INSERT/DELETE trên MongoDB,
#                       có sẵn 8 câu lệnh mẫu (sqlpad-queries/)
#   mongo-express 1.0.2 – thêm/sửa/xóa document MongoDB (database joblens, user ứng dụng)
# Cần chạy trước: deploy/mongodb, deploy/trino. Chạy từ gốc repo:  bash deploy/data-tools/install.sh
# Mở bằng scripts/demo-ui.sh: SQLPad http://localhost:3001, mongo-express http://localhost:8083.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"
NS=serving
rand() { python3 -c "import secrets,string,sys; a=string.ascii_letters+string.digits; print(''.join(secrets.choice(a) for _ in range(int(sys.argv[1]))))" "$1"; }
pw() { kubectl get secret "$1" -n "$NS" -o jsonpath="{.data.$2}" | base64 -d; }

kubectl get secret joblens-mongo -n "$NS" >/dev/null 2>&1 \
  || { echo "Chưa có Secret joblens-mongo – chạy bash deploy/mongodb/install.sh trước"; exit 1; }
if ! kubectl get secret sqlpad-admin -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic sqlpad-admin -n "$NS" \
    --from-literal=password="$(rand 16)" --from-literal=cookie-secret="$(rand 40)"
fi
if ! kubectl get secret mongo-express-auth -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic mongo-express-auth -n "$NS" \
    --from-literal=password="$(rand 16)" --from-literal=session-secret="$(rand 40)"
fi
kubectl create configmap sqlpad-queries -n "$NS" --from-file="${HERE}/sqlpad-queries/" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

EXISTED=$(kubectl get deployment sqlpad -n "$NS" -o name 2>/dev/null || true)
kubectl apply -f "${HERE}/sqlpad.yaml" -f "${HERE}/mongo-express.yaml"
# câu lệnh mẫu chỉ nạp khi SQLPad khởi động → khởi động lại khi chạy lại script (lần đầu không cần: pod mới đã nạp)
[ -z "$EXISTED" ] || kubectl rollout restart deployment/sqlpad -n "$NS" >/dev/null
echo "==> Lần đầu phải tải image (~150–250 MB mỗi image) – có thể mất vài phút tùy mạng"
echo "==> Chờ SQLPad và mongo-express"
kubectl rollout status deployment/sqlpad -n "$NS" --timeout=5m
kubectl rollout status deployment/mongo-express -n "$NS" --timeout=5m
echo
echo "Xong. Mở bằng: bash scripts/demo-ui.sh"
echo "  SQLPad        http://localhost:3001   đăng nhập admin@joblens.local / $(pw sqlpad-admin password)"
echo "  mongo-express http://localhost:8083   đăng nhập admin / $(pw mongo-express-auth password)"
echo "(Không dán mật khẩu vào nhóm chat. Xem lại: bash deploy/data-tools/install.sh – chạy lại an toàn.)"
