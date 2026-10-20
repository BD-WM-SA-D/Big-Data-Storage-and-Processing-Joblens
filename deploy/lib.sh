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

# image_tag NAME VERSION PATH... – tag dạng NAME:VERSION-gitsha (thêm -dirty nếu PATH có thay đổi chưa commit).
image_tag() {
  local name="$1" version="$2"; shift 2
  echo "${name}:${version}-$(git -C "$REPO_ROOT" rev-parse --short HEAD)$(git -C "$REPO_ROOT" diff --quiet HEAD -- "$@" || echo -dirty)"
}

# build_import TAG DOCKERFILE CONTEXT – build image rồi nạp vào cụm k3d "joblens" (không cần registry).
build_import() {
  echo "==> Build image $1"
  docker build -q -t "$1" -f "$2" "$3"
  k3d image import "$1" -c joblens
}

# spark_image – build + nạp image Spark job, in tag ra stdout (log build ra stderr).
spark_image() {
  local tag; tag="$(image_tag joblens/spark-jobs 0.1.0 spark-jobs dbt/seeds)"
  build_import "$tag" "${REPO_ROOT}/spark-jobs/Dockerfile" "$REPO_ROOT" >&2
  echo "$tag"
}
