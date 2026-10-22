#!/usr/bin/env bash
# Build image Spark job, nạp vào cụm k3d rồi chạy job bằng spark-submit --master k8s:// (cluster mode).
# Chạy từ gốc repo (cần .venv có pyspark – uv sync):
#   bash deploy/spark/submit.sh bronze_jobs --once     # xử lý hết dữ liệu đang có trong Kafka rồi dừng
#   bash deploy/spark/submit.sh bronze_jobs            # chạy liên tục, micro-batch mỗi 1 phút
#   bash deploy/spark/submit.sh silver_job_postings    # bronze → silver.job_postings + snapshot
#   DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze   # nạp VietJobs (đường A)
# Tham số sau tên job được chuyển nguyên cho job. Biến môi trường tùy chọn: DRIVER_MEMORY, EXECUTOR_MEMORY,
# EXECUTORS (mặc định 1g, 1g, 1), JOBLENS_USD_VND (tỷ giá quy đổi lương USD).
#
# EXECUTORS=0 (mặc định khi JOBLENS_PROFILE=lite): chạy local mode – một pod duy nhất (Job trong compute) chạy
# spark-submit --master local[2], không có pod executor riêng → tiết kiệm ~0,7–1 GB RAM cho máy 8 GB. Mã job,
# image, bảng Iceberg giống hệt; chỉ khác chỗ chạy executor. Ctrl+C chỉ dừng xem log, job vẫn chạy trong cụm.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"

JOB="${1:?Thiếu tên job, ví dụ: bronze_jobs}"; shift
NS=compute
[ -f "${REPO_ROOT}/spark-jobs/${JOB}.py" ] || { echo "Không có spark-jobs/${JOB}.py"; exit 1; }

load_env
if [ "$PROFILE" = lite ]; then : "${EXECUTORS:=0}"; fi
kubectl apply -f "${HERE}/rbac.yaml" >/dev/null
s3_secret "$NS"

TAG="$(spark_image)"

# Địa chỉ dịch vụ trong cụm (DNS đầy đủ vì driver chạy ở namespace compute)
KAFKA=joblens-kafka-bootstrap.ingest.svc.cluster.local:9092
CATALOG_URI=http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog
S3_ENDPOINT=http://seaweedfs-all-in-one.lakehouse.svc.cluster.local:8333
API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
PREFIX="$(echo "$JOB" | tr '_' '-')"
APP="${PREFIX}-$(date +%Y%m%d%H%M%S)"

if [ "${EXECUTORS:-1}" = 0 ]; then
  echo "==> Local mode (EXECUTORS=0): Job ${NS}/${APP}"
  ARGS=""
  for a in "$@"; do ARGS+=", $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$a")"; done
  kubectl apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${APP}
  namespace: ${NS}
  labels:
    app.kubernetes.io/part-of: joblens
    joblens-spark-app-prefix: ${PREFIX}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        spark-role: driver
        joblens-spark-app-prefix: ${PREFIX}
    spec:
      restartPolicy: Never
      serviceAccountName: spark
      containers:
        - name: spark
          image: ${TAG}
          imagePullPolicy: IfNotPresent
          command: ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--name", "${APP}",
                    "--driver-memory", "${DRIVER_MEMORY:-1g}",
                    "--conf", "spark.sql.shuffle.partitions=4",
                    "/opt/joblens/${JOB}.py"${ARGS}]
          env:
            - {name: JOBLENS_KAFKA_BOOTSTRAP, value: "${KAFKA}"}
            - {name: JOBLENS_CATALOG_URI, value: "${CATALOG_URI}"}
            - {name: JOBLENS_CATALOG_WAREHOUSE, value: "${JOBLENS_CATALOG_WAREHOUSE:-joblens}"}
            - {name: JOBLENS_S3_ENDPOINT, value: "${S3_ENDPOINT}"}
            - {name: JOBLENS_CHECKPOINT_DIR, value: /checkpoints}
            - {name: JOBLENS_USD_VND, value: "${JOBLENS_USD_VND:-26000}"}
          envFrom:
            - secretRef: {name: joblens-s3}
          resources:
            requests: {cpu: 250m, memory: 1Gi}
            limits: {memory: 2Gi}
          volumeMounts:
            - {name: ckpt, mountPath: /checkpoints}
      volumes:
        - name: ckpt
          persistentVolumeClaim: {claimName: spark-checkpoints}
EOF
  kubectl wait pod -n "$NS" -l "job-name=${APP}" --for=condition=Ready --timeout=300s >/dev/null 2>&1 || true
  kubectl logs -n "$NS" -f "job/${APP}" || true
  if kubectl wait -n "$NS" "job/${APP}" --for=condition=Complete --timeout=30s >/dev/null 2>&1; then
    echo "==> Xong: ${APP}"
  else
    echo "Job chưa xong hoặc lỗi – xem: kubectl get pod -n ${NS} -l job-name=${APP}; kubectl logs -n ${NS} job/${APP}"
    exit 1
  fi
  exit 0
fi

# Cluster mode: spark-submit chạy trên máy dev (pyspark trong .venv)
SPARK_SUBMIT="${REPO_ROOT}/.venv/bin/spark-submit"
[ -x "${REPO_ROOT}/.venv/bin/python" ] || { echo "Chưa có .venv – chạy 'uv sync' ở gốc repo"; exit 1; }
SPARK_HOME="$("${REPO_ROOT}/.venv/bin/python" -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")"; export SPARK_HOME
[ -x "$SPARK_SUBMIT" ] || { echo "Không thấy $SPARK_SUBMIT – chạy 'uv sync' ở gốc repo"; exit 1; }

envconf=()
for side in driverEnv executorEnv; do
  prefix=$([ "$side" = driverEnv ] && echo spark.kubernetes.driverEnv || echo spark.executorEnv)
  envconf+=(--conf "${prefix}.JOBLENS_KAFKA_BOOTSTRAP=${KAFKA}"
            --conf "${prefix}.JOBLENS_CATALOG_URI=${CATALOG_URI}"
            --conf "${prefix}.JOBLENS_CATALOG_WAREHOUSE=${JOBLENS_CATALOG_WAREHOUSE:-joblens}"
            --conf "${prefix}.JOBLENS_S3_ENDPOINT=${S3_ENDPOINT}"
            --conf "${prefix}.JOBLENS_S3_REGION=${JOBLENS_S3_REGION}"
            --conf "${prefix}.JOBLENS_CHECKPOINT_DIR=/checkpoints"
            --conf "${prefix}.JOBLENS_USD_VND=${JOBLENS_USD_VND:-26000}")
done
for role in driver executor; do
  for k in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION; do
    envconf+=(--conf "spark.kubernetes.${role}.secretKeyRef.${k}=joblens-s3:${k}")
  done
done

# Module dùng chung (normalize, bronze_jobs…) gửi kèm cho executor
PY_FILES=$(cd "${REPO_ROOT}/spark-jobs" && ls ./*.py | sed 's|^\./|local:///opt/joblens/|' | paste -sd, -)

echo "==> spark-submit ${APP}"
"$SPARK_SUBMIT" \
  --master "k8s://${API_SERVER}" --deploy-mode cluster --name "$APP" \
  --conf spark.kubernetes.namespace="$NS" \
  --conf spark.kubernetes.container.image="$TAG" \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.ckpt.options.claimName=spark-checkpoints \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.ckpt.mount.path=/checkpoints \
  --conf spark.executor.instances="${EXECUTORS:-1}" \
  --conf spark.driver.memory="${DRIVER_MEMORY:-1g}" --conf spark.executor.memory="${EXECUTOR_MEMORY:-1g}" \
  --conf spark.kubernetes.driver.request.cores=250m --conf spark.kubernetes.executor.request.cores=250m \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  "${envconf[@]}" \
  --py-files "$PY_FILES" \
  "local:///opt/joblens/${JOB}.py" "$@"

echo "Log driver: kubectl logs -n $NS \$(kubectl get pod -n $NS -l spark-role=driver --sort-by=.metadata.creationTimestamp -o name | tail -1) --tail=50"
