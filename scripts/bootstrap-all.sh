#!/usr/bin/env bash
# Dựng toàn bộ nền tảng từ đầu trên một máy trống (Codespaces, VM cloud, máy mới) theo đúng thứ tự deploy/README.md.
# Chạy lại an toàn: mỗi bước tự bỏ qua phần đã có. Lần đầu ~30–40 phút (tải image, tải VietJobs, chạy job).
#   bash scripts/bootstrap-all.sh            # tất cả
#   SKIP_DATA=1 bash scripts/bootstrap-all.sh   # chỉ hạ tầng, không nạp dữ liệu
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
step() { echo; echo "########## $* ##########"; }

step "0. Cụm k3d (profile ${JOBLENS_PROFILE:-full}, agent ${K3D_AGENTS:-2})"
if ! k3d cluster list joblens >/dev/null 2>&1; then bash deploy/k3d/create-cluster.sh; fi
[ -f .env ] || cp .env.example .env

step "1. Kafka";                bash deploy/kafka/install.sh
step "2. SeaweedFS + Lakekeeper"; bash deploy/storage/install.sh
step "3. MongoDB";              bash deploy/mongodb/install.sh
step "4. Trino (iceberg + mongodb)"; bash deploy/trino/install.sh

if [ -z "${SKIP_DATA:-}" ]; then
  step "5. VietJobs → S3";      bash deploy/data/upload-vietjobs.sh
  if [ "$(TRINO_FORMAT=CSV_UNQUOTED bash scripts/trino.sh "SELECT count(*) FROM iceberg.bronze.jobs_raw WHERE kafka_topic = 'backfill.vietjobs'" 2>/dev/null | tail -1 || echo 0)" -gt 0 ] 2>/dev/null; then
    echo "   bronze đã có VietJobs – bỏ qua nạp batch"
  else
    step "6. VietJobs → bronze (batch)"; DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze
  fi
  step "7. bronze → silver";    bash deploy/spark/submit.sh silver_job_postings
  step "8. dbt gold";           bash scripts/dbt.sh build
fi

step "9. Grafana + Kafka UI";   bash deploy/monitoring/install.sh
step "10. Airflow";             bash deploy/airflow/install.sh
step "Xong"
echo "Mở giao diện: bash scripts/demo-ui.sh   (Codespaces: tab PORTS → mở cổng 3000, 8082 …)"
