#!/usr/bin/env bash
# Build image Spark job, nạp vào cụm k3d rồi chạy job bằng spark-submit --master k8s:// (cluster mode).
# Chạy từ gốc repo (cần .venv có pyspark – uv sync):
#   bash deploy/spark/submit.sh bronze_jobs --once     # xử lý hết dữ liệu đang có trong Kafka rồi dừng
#   bash deploy/spark/submit.sh bronze_jobs            # chạy liên tục, micro-batch mỗi 1 phút
# Tham số sau tên job được chuyển nguyên cho job.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"

JOB="${1:?Thiếu tên job, ví dụ: bronze_jobs}"; shift
NS=compute
CLUSTER=joblens
VERSION=0.1.0
TAG="joblens/spark-jobs:${VERSION}-$(git -C "$REPO_ROOT" rev-parse --short HEAD)$(git -C "$REPO_ROOT" diff --quiet -- spark-jobs || echo -dirty)"
SPARK_SUBMIT="${REPO_ROOT}/.venv/bin/spark-submit"
SPARK_HOME="$("${REPO_ROOT}/.venv/bin/python" -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")"; export SPARK_HOME
[ -x "$SPARK_SUBMIT" ] || { echo "Không thấy $SPARK_SUBMIT – chạy 'uv sync' ở gốc repo"; exit 1; }
[ -f "${REPO_ROOT}/spark-jobs/${JOB}.py" ] || { echo "Không có spark-jobs/${JOB}.py"; exit 1; }

load_env
kubectl apply -f "${HERE}/rbac.yaml" >/dev/null
s3_secret "$NS"

echo "==> Build image ${TAG}"
docker build -q -t "$TAG" "${REPO_ROOT}/spark-jobs"
k3d image import "$TAG" -c "$CLUSTER"

# Địa chỉ dịch vụ trong cụm (DNS đầy đủ vì driver chạy ở namespace compute)
KAFKA=joblens-kafka-bootstrap.ingest.svc.cluster.local:9092
CATALOG_URI=http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog
S3_ENDPOINT=http://seaweedfs-all-in-one.lakehouse.svc.cluster.local:8333
API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
APP="$(echo "$JOB" | tr '_' '-')-$(date +%Y%m%d%H%M%S)"

envconf=()
for side in driverEnv executorEnv; do
  prefix=$([ "$side" = driverEnv ] && echo spark.kubernetes.driverEnv || echo spark.executorEnv)
  envconf+=(--conf "${prefix}.JOBLENS_KAFKA_BOOTSTRAP=${KAFKA}"
            --conf "${prefix}.JOBLENS_CATALOG_URI=${CATALOG_URI}"
            --conf "${prefix}.JOBLENS_CATALOG_WAREHOUSE=${JOBLENS_CATALOG_WAREHOUSE:-joblens}"
            --conf "${prefix}.JOBLENS_S3_ENDPOINT=${S3_ENDPOINT}"
            --conf "${prefix}.JOBLENS_S3_REGION=${JOBLENS_S3_REGION}"
            --conf "${prefix}.JOBLENS_CHECKPOINT_DIR=/checkpoints")
done
for role in driver executor; do
  for k in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION; do
    envconf+=(--conf "spark.kubernetes.${role}.secretKeyRef.${k}=joblens-s3:${k}")
  done
done

echo "==> spark-submit ${APP}"
"$SPARK_SUBMIT" \
  --master "k8s://${API_SERVER}" --deploy-mode cluster --name "$APP" \
  --conf spark.kubernetes.namespace="$NS" \
  --conf spark.kubernetes.container.image="$TAG" \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.ckpt.options.claimName=spark-checkpoints \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.ckpt.mount.path=/checkpoints \
  --conf spark.executor.instances=1 \
  --conf spark.driver.memory=1g --conf spark.executor.memory=1g \
  --conf spark.kubernetes.driver.request.cores=250m --conf spark.kubernetes.executor.request.cores=250m \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  "${envconf[@]}" \
  "local:///opt/joblens/${JOB}.py" "$@"

echo "Log driver: kubectl logs -n $NS \$(kubectl get pod -n $NS -l spark-role=driver --sort-by=.metadata.creationTimestamp -o name | tail -1) --tail=50"
