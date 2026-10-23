#!/usr/bin/env bash
# Tạo cụm k3d "joblens" theo cỡ máy (JOBLENS_PROFILE, mặc định full):
#   full – 1 server + 2 agent (giống cụm thật nhiều node; máy ≥ 16 GB)
#   lite – 1 server, 0 agent, tắt Traefik (dự án không dùng ingress) → bớt ~0,8 GB RAM (máy 8 GB)
# Chạy:  JOBLENS_PROFILE=lite bash deploy/k3d/create-cluster.sh
# Cụm đang có thì script dừng – muốn đổi cỡ phải xóa cụm (mất dữ liệu trong cụm):  k3d cluster delete joblens
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"

K3S_IMAGE=rancher/k3s:v1.35.5-k3s1   # bản đang dùng trong nhóm (kubectl get nodes) – không dùng latest

if k3d cluster list joblens >/dev/null 2>&1; then
  echo "Cụm 'joblens' đã có. Xóa trước nếu muốn tạo lại: k3d cluster delete joblens"; exit 1
fi
if [ "$PROFILE" = lite ]; then
  k3d cluster create joblens --image "$K3S_IMAGE" --servers 1 --agents 0 \
    --k3s-arg "--disable=traefik@server:0"
else
  # K3D_AGENTS: số node agent (mặc định 2). GitHub Codespaces đặt 0 để tiết kiệm đĩa (mỗi node giữ một bản image)
  k3d cluster create joblens --image "$K3S_IMAGE" --servers 1 --agents "${K3D_AGENTS:-2}"
fi
kubectl get nodes
echo "Tiếp theo: các bước trong deploy/README.md (kafka → storage → trino → …), giữ JOBLENS_PROFILE=${PROFILE}."
