#!/usr/bin/env bash
# Tải VietJobs (Pham Dinh et al., LREC 2026 – https://github.com/VinNLP/VietJobs, MIT;
# dữ liệu trên Hugging Face dinhieufam/VietJobs) rồi đưa lên s3://joblens/raw/vietjobs/ trong cụm.
# File tải về nằm ở data/vietjobs/ (đã gitignore) – KHÔNG commit dữ liệu.
# Chạy từ gốc repo, sau deploy/storage/install.sh:  bash deploy/data/upload-vietjobs.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "${HERE}/../lib.sh"
load_env

DEST_DIR="${REPO_ROOT}/data/vietjobs"
KEY=raw/vietjobs/VietJobs.csv
mkdir -p "$DEST_DIR"

echo "==> Tải VietJobs từ Hugging Face (chỉ file CSV)"
uv run --quiet --with "huggingface_hub>=0.25" python - "$DEST_DIR" <<'PY'
import sys
from huggingface_hub import snapshot_download
snapshot_download(repo_id="dinhieufam/VietJobs", repo_type="dataset", local_dir=sys.argv[1], allow_patterns=["*.csv"])
PY
CSV=$(find "$DEST_DIR" -name '*.csv' -type f -printf '%s %p\n' | sort -nr | head -1 | cut -d' ' -f2-)
[ -n "$CSV" ] || { echo "Không thấy file CSV trong $DEST_DIR"; exit 1; }
echo "    $CSV ($(du -h "$CSV" | cut -f1)), sha256 $(sha256sum "$CSV" | cut -c1-16)…"

echo "==> Đưa lên s3://${JOBLENS_S3_BUCKET:-joblens}/${KEY}"
kubectl port-forward -n lakehouse svc/seaweedfs-all-in-one 18333:8333 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null || true' EXIT
sleep 3
uv run --quiet --with "boto3>=1.35" python - "$CSV" "${JOBLENS_S3_BUCKET:-joblens}" "$KEY" <<'PY'
import os, sys
import boto3
from botocore.config import Config
path, bucket, key = sys.argv[1:4]
# SeaweedFS: bắt buộc path-style (http://host/bucket/key) – kiểu mặc định đưa bucket vào tên miền
s3 = boto3.client("s3", endpoint_url="http://localhost:18333", region_name=os.environ["JOBLENS_S3_REGION"],
                  config=Config(s3={"addressing_style": "path"}),
                  aws_access_key_id=os.environ["JOBLENS_S3_ACCESS_KEY_ID"],
                  aws_secret_access_key=os.environ["JOBLENS_S3_SECRET_ACCESS_KEY"])
s3.upload_file(path, bucket, key)
size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
print(f"    đã tải lên {size:,} byte → s3://{bucket}/{key}")
PY
echo "Xong. Nạp vào bronze:  DRIVER_MEMORY=2g bash deploy/spark/submit.sh vietjobs --sink bronze"
