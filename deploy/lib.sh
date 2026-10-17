# shellcheck shell=bash
# Hàm dùng chung cho các script cài đặt trong deploy/ – nạp bằng:  . "$(dirname "$0")/../lib.sh"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

# Nạp .env (khóa S3, địa chỉ dịch vụ…). .env đã gitignore, tạo từ .env.example.
load_env() {
  [ -f "$ENV_FILE" ] || { echo "Thiếu .env – chạy: cp .env.example .env"; exit 1; }
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
  export JOBLENS_S3_REGION="${JOBLENS_S3_REGION:-us-east-1}"
}

# s3_secret NAMESPACE – tạo/cập nhật Secret "joblens-s3" chứa khóa S3 để pod trong namespace đó dùng.
s3_secret() {
  [ -n "${JOBLENS_S3_ACCESS_KEY_ID:-}" ] || { echo "Chưa có khóa S3 trong .env – chạy deploy/storage/install.sh trước"; exit 1; }
  kubectl create namespace "$1" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl create secret generic joblens-s3 -n "$1" \
    --from-literal=JOBLENS_S3_ACCESS_KEY_ID="$JOBLENS_S3_ACCESS_KEY_ID" \
    --from-literal=JOBLENS_S3_SECRET_ACCESS_KEY="$JOBLENS_S3_SECRET_ACCESS_KEY" \
    --from-literal=JOBLENS_S3_REGION="$JOBLENS_S3_REGION" \
    --from-literal=AWS_ACCESS_KEY_ID="$JOBLENS_S3_ACCESS_KEY_ID" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$JOBLENS_S3_SECRET_ACCESS_KEY" \
    --from-literal=AWS_REGION="$JOBLENS_S3_REGION" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}
