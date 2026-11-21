#!/usr/bin/env bash
# Ghép Web Mining vào nền tảng (issue #2, ADR 0001 D3):
#   1. clone repo WM đúng commit WM_REF (deploy/mining/images.env) vào ~/.cache/joblens/, build 3 image bản stub
#      (không model, không torch – nhỏ, vài giây) bằng Dockerfile của WM, nạp vào cụm k3d;
#   2. tạo bronze.reviews_raw và 3 bảng silver của WM (Spark job bronze_reviews – image WM không tự tạo bảng);
#   3. ghi chế độ cho DAG vào ConfigMap joblens-mining (orchestration) rồi cài lại Airflow để DAG nhận Variable.
# Chạy từ gốc repo:  bash deploy/mining/install.sh [stub|full|off]   (mặc định stub)
#   full: image có model do WM build trên máy WM (cần thư mục data/ không có trong git) – WM gửi file image
#         (docker save) qua kênh nội bộ rồi: docker load -i <file> && k3d image import <tag> -c joblens
#   off:  tắt 3 task mô hình trong DAG (gold vẫn chạy, các bảng WM để rỗng)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../lib.sh
. "${HERE}/../lib.sh"
# shellcheck source=images.env
. "${HERE}/images.env"
MODE="${1:-stub}"
case "$MODE" in stub|full|off) ;; *) echo "Dùng: $0 [stub|full|off]"; exit 1 ;; esac

SRC="${XDG_CACHE_HOME:-$HOME/.cache}/joblens/web-mining-${WM_REF}"
SHA=""
if [ "$MODE" != off ]; then
  echo "==> Mã nguồn Web Mining @ ${WM_REF}"
  if [ ! -d "$SRC/.git" ]; then
    git clone -q "$WM_REPO" "$SRC"
  fi
  git -C "$SRC" fetch -q origin
  git -C "$SRC" checkout -q "$WM_REF"
  SHA="$(git -C "$SRC" rev-parse --short=7 HEAD)"
fi

SUFFIX=$([ "$MODE" = stub ] && echo "-stub" || echo "")
declare -A VERSION=([skill_extractor]="$SKILL_EXTRACTOR_VERSION" [job_dedup]="$JOB_DEDUP_VERSION"
                    [absa_scorer]="$ABSA_SCORER_VERSION")
declare -A TAG
for job in skill_extractor job_dedup absa_scorer; do
  name="joblens/${job//_/-}"
  TAG[$job]="${name}:${VERSION[$job]}-${SHA:-none}${SUFFIX}"
  if [ "$MODE" = stub ]; then
    echo "==> Build ${TAG[$job]} (Dockerfile của WM, target stub)"
    docker build -q -f "$SRC/images/batch/Dockerfile" --target stub --build-arg "JOB=$job" \
      --build-arg "VERSION=${VERSION[$job]}-${SHA}" -t "${TAG[$job]}" "$SRC" >/dev/null
    k3d image import "${TAG[$job]}" -c joblens >/dev/null && echo "    đã nạp vào cụm"
  elif [ "$MODE" = full ] && ! docker image inspect "${TAG[$job]}" >/dev/null 2>&1; then
    echo "Chưa có ${TAG[$job]} trên máy – nhận file image từ WM, docker load, rồi chạy lại." >&2; exit 1
  elif [ "$MODE" = full ]; then
    k3d image import "${TAG[$job]}" -c joblens >/dev/null && echo "==> Đã nạp ${TAG[$job]}"
  fi
done

echo "==> Bảng bronze.reviews_raw + silver.job_skills / job_dedup / review_aspects (Spark job bronze_reviews)"
EXECUTORS=0 DRIVER_MEMORY="${DRIVER_MEMORY:-1g}" bash "${REPO_ROOT}/deploy/spark/submit.sh" bronze_reviews

echo "==> ConfigMap joblens-mining (chế độ + tag cho DAG)"
kubectl create configmap joblens-mining -n orchestration \
  --from-literal=mode="$MODE" \
  --from-literal=skill_extractor="${TAG[skill_extractor]}" \
  --from-literal=job_dedup="${TAG[job_dedup]}" \
  --from-literal=absa_scorer="${TAG[absa_scorer]}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl get configmap joblens-mining -n orchestration -o jsonpath='{.data}'; echo

echo "==> Cài lại Airflow để DAG nhận Variable mới (joblens_mining_*)"
bash "${REPO_ROOT}/deploy/airflow/install.sh"
echo "Xong. DAG joblens_pipeline: bronze_reviews → … → mining_skills → mining_dedup → mining_absa → dbt_build_gold (chế độ $MODE)."
