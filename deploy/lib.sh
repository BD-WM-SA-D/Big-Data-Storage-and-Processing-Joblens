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
# Bỏ qua bước nạp (mất vài phút với image Spark ~2 GB) nếu mọi node đã có đúng image đó.
build_import() {
  echo "==> Build image $1"
  docker build -q -t "$1" -f "$2" "$3"
  local id node missing=0
  id="$(docker image inspect -f '{{.Id}}' "$1")"
  for node in $(docker ps --format '{{.Names}}' | grep -E '^k3d-joblens-(server|agent)-[0-9]+$'); do
    docker exec "$node" crictl inspecti -o go-template --template '{{.status.id}}' "docker.io/$1" 2>/dev/null \
      | grep -qx "$id" || missing=1
  done
  if [ "$missing" = 0 ]; then
    echo "    image đã có trên mọi node – bỏ qua k3d image import"
  else
    k3d image import "$1" -c joblens
  fi
}

# spark_image – build + nạp image Spark job, in tag ra stdout (log build ra stderr).
spark_image() {
  local tag; tag="$(image_tag joblens/spark-jobs 0.1.0 spark-jobs dbt/seeds)"
  build_import "$tag" "${REPO_ROOT}/spark-jobs/Dockerfile" "$REPO_ROOT" >&2
  echo "$tag"
}

# Cấu hình theo cỡ máy: JOBLENS_PROFILE=full (mặc định, máy ≥ 16 GB) | lite (máy 8 GB) – xem deploy/README.md.
# Đặt bằng:  echo 'export JOBLENS_PROFILE=lite' >> ~/.bashrc   (không ghi vào .env vì .env đồng bộ từ repo joblens)
PROFILE="${JOBLENS_PROFILE:-full}"
case "$PROFILE" in full|lite) ;; *) echo "JOBLENS_PROFILE phải là full hoặc lite (đang là '$PROFILE')"; exit 1;; esac

# profile_values DIR – in "-f DIR/values-lite.yaml" khi PROFILE=lite và file tồn tại (thêm vào lệnh helm)
profile_values() {
  if [ "$PROFILE" = lite ] && [ -f "$1/values-lite.yaml" ]; then echo "-f $1/values-lite.yaml"; fi
}

# stop_spark_job PREFIX – dừng job Spark có tên bắt đầu bằng PREFIX trong namespace compute (cả hai chế độ:
# cluster mode → pod driver/executor; local mode → Job do submit.sh tạo). Ví dụ: stop_spark_job bronze-jobs
stop_spark_job() {
  local pods
  kubectl delete job -n compute -l "joblens-spark-app-prefix=$1" --wait=false >/dev/null 2>&1 || true
  pods=$(kubectl get pod -n compute -o name 2>/dev/null | grep "^pod/$1-" || true)
  if [ -n "$pods" ]; then
    # shellcheck disable=SC2086
    kubectl delete -n compute $pods --wait=false >/dev/null
    echo "   đã dừng: $(echo "$pods" | tr '\n' ' ')"
  fi
}
