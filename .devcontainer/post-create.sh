#!/usr/bin/env bash
# Cài công cụ cho Codespaces (chạy một lần khi tạo codespace). Phiên bản khớp máy dev của nhóm (deploy/compat.md).
set -euo pipefail
KUBECTL=v1.35.9
HELM=v3.22.1
K3D=v5.9.0
UV=0.13.5
BIN=/usr/local/bin

echo "==> kubectl ${KUBECTL}, helm ${HELM}, k3d ${K3D}, uv ${UV}"
sudo curl -fsSL -o "${BIN}/kubectl" "https://dl.k8s.io/release/${KUBECTL}/bin/linux/amd64/kubectl"
sudo chmod +x "${BIN}/kubectl"
curl -fsSL "https://get.helm.sh/helm-${HELM}-linux-amd64.tar.gz" | sudo tar xz -C "$BIN" --strip-components=1 linux-amd64/helm
sudo curl -fsSL -o "${BIN}/k3d" "https://github.com/k3d-io/k3d/releases/download/${K3D}/k3d-linux-amd64"
sudo chmod +x "${BIN}/k3d"
curl -LsSf "https://astral.sh/uv/${UV}/install.sh" | sh

export PATH="${HOME}/.local/bin:${PATH}"
[ -f .env ] || cp .env.example .env
uv sync
echo
echo "Xong. Dựng toàn bộ nền tảng (~30–40 phút lần đầu):  bash scripts/bootstrap-all.sh"
