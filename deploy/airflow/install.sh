#!/usr/bin/env bash
# Cài Airflow 3.3.2 (chart 1.22.0) vào namespace "orchestration" và nạp DAG joblens_pipeline.
# Cần chạy trước: deploy/kafka, deploy/storage, deploy/trino (DAG gọi Spark + Trino trong cụm).
# Chạy từ gốc repo:  bash deploy/airflow/install.sh   (chạy lại nhiều lần vẫn an toàn)
#
# Mỗi lần chạy: build image spark-jobs + dbt theo git commit hiện tại, nạp vào k3d, cập nhật Airflow
# Variables joblens_spark_image / joblens_dbt_image và ConfigMap DAG. Sửa DAG xong chỉ cần chạy lại script này.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"

AIRFLOW_CHART_VERSION=1.22.0   # ứng dụng Airflow 3.3.2 – đặt trong values.yaml (joblens/versions.md: 3.3.x)
NS=orchestration

rand() { python3 -c "import secrets,string,sys; a=string.ascii_letters+string.digits; print(''.join(secrets.choice(a) for _ in range(int(sys.argv[1]))))" "$1"; }
secret_val() { kubectl get secret "$1" -n "$NS" -o jsonpath="{.data.$2}" | base64 -d; }

load_env
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "${REPO_ROOT}/deploy/spark/rbac.yaml" >/dev/null   # namespace compute + SA spark
s3_secret compute

echo "==> Image cho task của DAG"
SPARK_IMAGE="$(spark_image)"
DBT_IMAGE="$(image_tag joblens/dbt 0.1.0 dbt)"
build_import "$DBT_IMAGE" "${REPO_ROOT}/dbt/Dockerfile" "${REPO_ROOT}/dbt"

echo "==> Secret (sinh một lần)"
if ! kubectl get secret airflow-pg -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic airflow-pg -n "$NS" --from-literal=password="$(rand 32)"
fi
PG_PASS="$(secret_val airflow-pg password)"
kubectl create secret generic airflow-metadata -n "$NS" \
  --from-literal=connection="postgresql://airflow:${PG_PASS}@airflow-postgres.${NS}.svc.cluster.local:5432/airflow" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
if ! kubectl get secret airflow-keys -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic airflow-keys -n "$NS" \
    --from-literal=fernet-key="$(python3 -c 'import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')" \
    --from-literal=api-secret-key="$(rand 48)" --from-literal=jwt-secret="$(rand 64)"
fi
if ! kubectl get secret airflow-admin -n "$NS" >/dev/null 2>&1; then
  kubectl create secret generic airflow-admin -n "$NS" --from-literal=password="$(rand 16)"
fi

echo "==> Postgres cho Airflow"
kubectl apply -f "${HERE}/postgres.yaml" >/dev/null
kubectl rollout status statefulset/airflow-postgres -n "$NS" --timeout=5m

echo "==> DAG (ConfigMap joblens-dags từ airflow/dags/)"
kubectl create configmap joblens-dags -n "$NS" --from-file="${REPO_ROOT}/airflow/dags/" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Airflow Variables cho DAG: tag image đổi theo commit nên sinh file values tạm (không commit)
VARS="$(mktemp --suffix=.yaml)"; trap 'rm -f "$VARS"' EXIT
cat > "$VARS" <<EOF
env:
  - name: AIRFLOW_VAR_JOBLENS_SPARK_IMAGE
    value: "${SPARK_IMAGE}"
  - name: AIRFLOW_VAR_JOBLENS_DBT_IMAGE
    value: "${DBT_IMAGE}"
  - name: AIRFLOW_VAR_JOBLENS_SPARK_EXECUTORS
    value: "$([ "$PROFILE" = lite ] && echo 0 || echo 1)"
EOF

echo "==> Airflow (chart ${AIRFLOW_CHART_VERSION})"
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update apache-airflow >/dev/null
# Job không phải hook thì không sửa tại chỗ được (spec bất biến) → xóa bản cũ, helm tạo lại (migrate chạy lại vẫn an toàn)
kubectl delete job -n "$NS" airflow-run-airflow-migrations airflow-create-user --ignore-not-found >/dev/null
# shellcheck disable=SC2046  # profile_values cố ý tách thành hai đối số (-f FILE)
helm upgrade --install airflow apache-airflow/airflow --version "$AIRFLOW_CHART_VERSION" -n "$NS" \
  -f "${HERE}/values.yaml" $(profile_values "$HERE") -f "$VARS" \
  --set-string createUserJob.defaultUser.password="$(secret_val airflow-admin password)" \
  --wait --timeout 15m
# subPath không tự cập nhật khi ConfigMap đổi → khởi động lại các thành phần đọc DAG
kubectl rollout restart statefulset/airflow-scheduler -n "$NS" >/dev/null 2>&1 \
  || kubectl rollout restart deployment/airflow-scheduler -n "$NS" >/dev/null
kubectl rollout restart deployment/airflow-dag-processor -n "$NS" >/dev/null

echo "==> Quyền tạo pod trong compute cho scheduler"
kubectl apply -f "${HERE}/rbac.yaml" >/dev/null

kubectl get pods -n "$NS"
cat <<EOF

Airflow UI:  kubectl port-forward -n ${NS} svc/airflow-api-server 8082:8080  →  http://localhost:8082
             (hoặc bash scripts/demo-ui.sh)  – đăng nhập: admin / $(secret_val airflow-admin password)
Image của DAG: ${SPARK_IMAGE}, ${DBT_IMAGE}
DAG joblens_pipeline mặc định đang tạm dừng: bật công tắc trên giao diện hoặc
  kubectl exec -n ${NS} statefulset/airflow-scheduler -c scheduler -- airflow dags unpause joblens_pipeline
EOF
