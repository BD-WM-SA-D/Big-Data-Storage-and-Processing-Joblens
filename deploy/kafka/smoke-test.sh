#!/usr/bin/env bash
# Kiểm tra nhanh: gửi 3 tin mẫu (joblens/fixtures/job_posting.v1.sample.json, dữ liệu tự soạn)
# vào jobs.raw.topcv.v1 với key = source_job_id, rồi đọc lại. Không dùng crawler thật.
# Chạy từ gốc repo:  bash deploy/kafka/smoke-test.sh [đường dẫn tới repo joblens]
set -euo pipefail

JOBLENS_DIR="${1:-../joblens}"
FIXTURE="${JOBLENS_DIR}/fixtures/job_posting.v1.sample.json"
IMAGE=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1
BOOTSTRAP=joblens-kafka-bootstrap:9092
TOPIC=jobs.raw.topcv.v1

[ -f "$FIXTURE" ] || { echo "Không thấy $FIXTURE – truyền đường dẫn repo joblens làm tham số"; exit 1; }

echo "==> Gửi $(python3 -c "import json,sys;print(len(json.load(open(sys.argv[1]))))" "$FIXTURE") bản ghi vào $TOPIC"
python3 -c '
import json, sys
for rec in json.load(open(sys.argv[1])):
    print(rec["source_job_id"] + "\t" + json.dumps(rec, ensure_ascii=False))
' "$FIXTURE" | kubectl run kafka-smoke-producer -n ingest -i --rm --restart=Never --quiet --image="$IMAGE" -- \
  bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --property parse.key=true --property "key.separator=	" --producer-property acks=all

echo "==> Đọc lại (key | partition | 120 ký tự đầu của value)"
kubectl run kafka-smoke-consumer -n ingest -i --rm --restart=Never --quiet --image="$IMAGE" -- \
  bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --from-beginning --max-messages 3 --timeout-ms 20000 \
  --property print.key=true --property print.partition=true \
  | cut -c1-160
