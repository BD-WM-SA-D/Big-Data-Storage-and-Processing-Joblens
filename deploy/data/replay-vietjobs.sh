#!/usr/bin/env bash
# Replay tin VietJobs vào topic jobs.raw.vietjobs.v1 (nguồn sự kiện cho đường streaming – demo, đo tải).
# Chạy một pod Python nhỏ (producer confluent-kafka, ~300 MB) thay vì job Spark – xem spark-jobs/vietjobs.py.
#   bash deploy/data/replay-vietjobs.sh                 # 3.000 tin, 50 tin/giây (~1 phút)
#   bash deploy/data/replay-vietjobs.sh 10000 200       # LIMIT RATE
#   bash deploy/data/replay-vietjobs.sh 3000 50 --late-every 10   # 1/10 tin có crawl_ts lùi 20 phút (thử watermark)
# Cần: deploy/data/upload-vietjobs.sh đã chạy (CSV trên s3://joblens/raw/vietjobs/), job streaming đang chạy
# (bash deploy/spark/submit.sh bronze_jobs …) nếu muốn thấy dữ liệu vào bronze.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"

LIMIT="${1:-3000}"
RATE="${2:-50}"
shift $(( $# < 2 ? $# : 2 ))
EXTRA=""
for a in "$@"; do EXTRA+=", $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$a")"; done
NS=compute
JOB="vietjobs-replay-$(date +%H%M%S)"

load_env
kubectl apply -f "${REPO_ROOT}/deploy/spark/rbac.yaml" >/dev/null
s3_secret "$NS"
TAG="$(spark_image)"

kubectl apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB}
  namespace: ${NS}
  labels:
    app.kubernetes.io/part-of: joblens
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels:
        app: vietjobs-replay
    spec:
      restartPolicy: Never
      containers:
        - name: replay
          image: ${TAG}
          imagePullPolicy: IfNotPresent
          command: ["python3", "/opt/joblens/vietjobs.py", "--sink", "kafka",
                    "--limit", "${LIMIT}", "--rate", "${RATE}", "--batch", "100"${EXTRA}]
          env:
            - name: JOBLENS_KAFKA_BOOTSTRAP
              value: joblens-kafka-bootstrap.ingest.svc.cluster.local:9092
            - name: JOBLENS_S3_ENDPOINT
              value: http://seaweedfs-all-in-one.lakehouse.svc.cluster.local:8333
          envFrom:
            - secretRef:
                name: joblens-s3
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              memory: 768Mi
          volumeMounts:
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: tmp
          emptyDir: {}
EOF

echo "==> Replay ${LIMIT} tin, ${RATE} tin/giây – job ${NS}/${JOB}"
kubectl wait pod -n "$NS" -l "job-name=${JOB}" --for=condition=Ready --timeout=180s >/dev/null 2>&1 || true
kubectl logs -n "$NS" -f "job/${JOB}"
if kubectl wait -n "$NS" "job/${JOB}" --for=condition=Complete --timeout=60s >/dev/null; then
  echo "==> Xong"
else
  echo "Job lỗi – xem: kubectl describe job -n $NS $JOB"; exit 1
fi
