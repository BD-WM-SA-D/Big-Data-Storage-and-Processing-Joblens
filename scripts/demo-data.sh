#!/usr/bin/env bash
# shellcheck disable=SC2016  # lệnh mongosh dùng $inc, $group… – cố ý để trong nháy đơn
# Thao tác dữ liệu trực tiếp khi giảng viên hỏi "thêm / sửa / xóa dữ liệu thử xem" (checkpoint, buổi demo).
# Mỗi lệnh in câu lệnh trước khi chạy rồi dừng chờ Enter để kịp giải thích (NONSTOP=1 để chạy một mạch).
#
#   bash scripts/demo-data.sh sql      # SQL trên Iceberg qua Trino: CREATE/INSERT/UPDATE/DELETE, snapshot, time travel
#   bash scripts/demo-data.sh nosql    # MongoDB: insert/find/update/delete/aggregate, rồi Trino đọc + join Iceberg
#   bash scripts/demo-data.sh kafka ["Tiêu đề"] ["15 - 25 triệu"] ["Hà Nội"]
#                                      # thêm MỘT tin mới đi hết pipeline: Kafka → bronze → silver (chuẩn hóa)
#   bash scripts/demo-data.sh clean    # xóa mọi thứ các lệnh trên tạo ra
#
# Chỉ ghi vào chỗ của repo này: namespace Iceberg `sandbox`, collection MongoDB `sandbox_jobs`, topic
# jobs.raw.vietjobs.v1 (tin source = synthetic, source_job_id bắt đầu bằng "demo-"). Không đụng bảng của repo khác.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

T=iceberg.sandbox.demo_jobs
TOPIC=jobs.raw.vietjobs.v1
KAFKA_IMAGE=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1   # cùng image với deploy/kafka/smoke-test.sh

pause() { [ -n "${NONSTOP:-}" ] || read -rp "   … Enter để tiếp " _ </dev/tty; }
title() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
q() {  # q "SQL" – in rồi chạy một câu lệnh Trino
  printf '\033[1;33mtrino>\033[0m %s\n' "$1"
  bash scripts/trino.sh "$1"
}
qv() { TRINO_FORMAT=CSV_UNQUOTED bash scripts/trino.sh "$1" | tail -1; }  # giá trị đơn

mongo_url() { kubectl get secret joblens-mongo -n serving -o jsonpath='{.data.JOBLENS_MONGO_URL}' | base64 -d; }
m() {  # m "lệnh mongosh" – in rồi chạy (user ứng dụng joblens, quyền readWrite trên database joblens)
  printf '\033[1;33mmongo>\033[0m %s\n' "$1"
  kubectl exec -n serving mongodb-0 -- mongosh --quiet "$(mongo_url)" --eval "printjson($1)"
}

sql_demo() {
  title "1. Tạo bảng Iceberg mới từ 5 tin silver (CTAS, phân vùng theo cấp bậc)"
  q "CREATE SCHEMA IF NOT EXISTS iceberg.sandbox"
  q "DROP TABLE IF EXISTS $T"
  q "CREATE TABLE $T WITH (partitioning = ARRAY['level_code']) AS
     SELECT job_uid, title, level_code, element_at(location_codes, 1) AS location_code, salary_min_vnd, salary_max_vnd
     FROM iceberg.silver.job_postings
     WHERE salary_min_vnd IS NOT NULL AND cardinality(location_codes) > 0 AND level_code IN ('junior', 'mid', 'senior')
     LIMIT 5"
  q "SELECT * FROM $T ORDER BY salary_min_vnd"
  pause

  title "2. INSERT – thêm một tin"
  q "INSERT INTO $T VALUES ('demo-sql-1', 'Kỹ sư dữ liệu (tin demo)', 'mid', '01', 20000000, 30000000)"
  q "SELECT * FROM $T WHERE job_uid = 'demo-sql-1'"
  pause

  title "3. UPDATE – sửa lương (Iceberg v2 ghi delete file + file mới, không ghi đè file cũ)"
  q "UPDATE $T SET salary_max_vnd = 35000000 WHERE job_uid = 'demo-sql-1'"
  q "SELECT job_uid, title, salary_min_vnd, salary_max_vnd FROM $T WHERE job_uid = 'demo-sql-1'"
  pause

  title "4. DELETE – xóa các tin lương dưới 10 triệu"
  q "SELECT count(*) AS truoc_khi_xoa FROM $T"
  q "DELETE FROM $T WHERE salary_min_vnd < 10000000"
  q "SELECT count(*) AS sau_khi_xoa FROM $T"
  pause

  title "5. Mỗi thao tác là một snapshot – time travel về lúc vừa tạo bảng"
  q "SELECT snapshot_id, committed_at, operation, element_at(summary, 'added-records') AS them,
            element_at(summary, 'deleted-records') AS xoa
     FROM iceberg.sandbox.\"demo_jobs\$snapshots\" ORDER BY committed_at"
  local first
  first=$(qv "SELECT snapshot_id FROM iceberg.sandbox.\"demo_jobs\$snapshots\" ORDER BY committed_at LIMIT 1")
  q "SELECT count(*) AS so_tin_luc_tao_bang FROM $T FOR VERSION AS OF $first"
  q "SELECT count(*) AS so_tin_hien_tai FROM $T"
  q "SELECT file_path, record_count, file_size_in_bytes FROM iceberg.sandbox.\"demo_jobs\$files\""
  echo "File Parquet ở trên nằm trên SeaweedFS (http://localhost:8888/buckets/joblens/ → warehouse/)."
}

nosql_demo() {
  title "1. Xóa bản cũ, insertMany – document có mảng kỹ năng lồng bên trong (không cần khai báo lược đồ)"
  m 'db.sandbox_jobs.drop()'
  m 'db.getCollection("_schema").deleteOne({table: "sandbox_jobs"})'  # để Trino đoán lại cột (xem docs/demo.md mục 5)
  m 'db.sandbox_jobs.insertMany([
    {_id: "demo-nosql-1", title: "Kỹ sư dữ liệu (tin demo)", location_code: "01", level_code: "mid",
     salary_min_vnd: 20000000, salary_max_vnd: 30000000, skills: ["Spark", "Kafka", "SQL"]},
    {_id: "demo-nosql-2", title: "Lập trình viên Python (tin demo)", location_code: "79", level_code: "junior",
     salary_min_vnd: 12000000, salary_max_vnd: 18000000, skills: ["Python", "Django"]},
    {_id: "demo-nosql-3", title: "Thực tập sinh dữ liệu (tin demo)", location_code: "01", level_code: "intern",
     salary_min_vnd: 3000000, salary_max_vnd: 5000000, skills: ["SQL"]}])'
  pause

  title "2. find – lọc theo trường lồng trong mảng"
  m 'db.sandbox_jobs.find({skills: "SQL"}, {title: 1, location_code: 1, _id: 0}).toArray()'
  pause

  title "3. updateOne – tăng lương, thêm kỹ năng"
  m 'db.sandbox_jobs.updateOne({_id: "demo-nosql-2"}, {$inc: {salary_max_vnd: 2000000}, $push: {skills: "FastAPI"}})'
  m 'db.sandbox_jobs.findOne({_id: "demo-nosql-2"})'
  pause

  title "4. deleteOne + aggregate – lương trung bình theo tỉnh"
  m 'db.sandbox_jobs.deleteOne({_id: "demo-nosql-3"})'
  m 'db.sandbox_jobs.aggregate([{$group: {_id: "$location_code", so_tin: {$sum: 1},
       luong_tb: {$avg: {$avg: ["$salary_min_vnd", "$salary_max_vnd"]}}}}]).toArray()'
  pause

  title "5. Trino đọc collection MongoDB và join với bảng Iceberg (truy vấn liên nguồn)"
  q "SELECT s._id, s.title, d.name AS tinh_thanh, s.salary_max_vnd
     FROM mongodb.joblens.sandbox_jobs s JOIN iceberg.gold.dim_location d ON d.location_code = s.location_code
     ORDER BY s._id"
}

kafka_demo() {
  local ttl="${1:-Kỹ sư dữ liệu Big Data (tin demo)}" sal="${2:-20 - 30 triệu}" loc="${3:-Hà Nội}"
  local id msg uid
  id="demo-$(date +%Y%m%d%H%M%S)"
  msg=$(python3 - "$id" "$ttl" "$sal" "$loc" <<'PY'
import hashlib, json, sys
from datetime import datetime, timezone
sid, title, salary, loc = sys.argv[1:]
payload = {"title": title, "company_name": "Công ty Demo", "company_id": None, "salary_text": salary,
           "locations": [loc], "experience_text": "2 năm", "level": "Nhân viên", "employment_type": "Toàn thời gian",
           "category": "IT phần mềm", "deadline": None, "date_posted": None, "skill_tags": ["Spark", "Kafka"],
           "description": "Tin tạo tay khi demo (không phải dữ liệu thật).", "requirements": None, "benefits": None}
canon = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
print(json.dumps({"schema_version": "job_posting.v1", "source": "synthetic", "source_job_id": sid, "url": None,
                  "crawl_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z",
                  "lastmod": None, "content_hash": hashlib.sha256(canon.encode()).hexdigest(),
                  "raw_html_uri": None, "payload": payload}, ensure_ascii=False))
PY
)
  uid=$(python3 -c 'import hashlib,sys; print(hashlib.sha1(("synthetic:" + sys.argv[1]).encode()).hexdigest())' "$id")

  title "1. Gửi 1 tin vào Kafka topic $TOPIC (key = $id)"
  echo "$msg"
  printf '%s\t%s\n' "$id" "$msg" | kubectl run "demo-producer-$(date +%s)" -n ingest -i --rm --restart=Never --quiet \
    --image="$KAFKA_IMAGE" -- bin/kafka-console-producer.sh --bootstrap-server joblens-kafka-bootstrap:9092 \
    --topic "$TOPIC" --property parse.key=true --property "key.separator=	" --producer-property acks=all
  echo "Đã gửi – xem trên Kafka UI: http://localhost:8081 → $TOPIC → Messages"
  pause

  title "2. Kafka → bronze (Spark Structured Streaming)"
  if kubectl get pods -n compute --no-headers 2>/dev/null | grep -E '^bronze-jobs-' | grep -q Running; then
    echo "Job bronze_jobs đang chạy – đợi micro-batch kế tiếp"
  else
    echo "Job bronze_jobs không chạy – chạy một lượt --once (đọc hết Kafka rồi dừng, ~1–2 phút)"
    DRIVER_MEMORY=768m EXECUTORS=0 bash deploy/spark/submit.sh bronze_jobs --once
  fi
  for _ in $(seq 1 30); do
    [ "$(qv "SELECT count(*) FROM iceberg.bronze.jobs_raw WHERE source = 'synthetic' AND source_job_id = '$id'")" -gt 0 ] && break
    sleep 10
  done
  q "SELECT kafka_topic, kafka_partition, kafka_offset, ingest_ts, source, source_job_id, crawl_ts
     FROM iceberg.bronze.jobs_raw WHERE source = 'synthetic' AND source_job_id = '$id'"
  pause

  title "3. bronze → silver (Spark batch MERGE INTO + chuẩn hóa lương/tỉnh/kinh nghiệm/cấp bậc)"
  DRIVER_MEMORY=1g EXECUTORS=0 bash deploy/spark/submit.sh silver_job_postings
  q "SELECT job_uid, title, salary_text, salary_min_vnd, salary_max_vnd, location_codes, experience_min_years, level_code
     FROM iceberg.silver.job_postings WHERE job_uid = '$uid'"
  echo "Gold (dbt) cố ý bỏ tin synthetic nên tin demo chỉ thấy ở bronze/silver; ô 'Tin trong silver' trên Grafana tăng 1."
}

clean() {
  title "Xóa dữ liệu demo"
  q "DROP TABLE IF EXISTS $T"
  q "DROP TABLE IF EXISTS iceberg.sandbox.web_jobs"       # bảng thử của SQLPad (deploy/data-tools)
  q "DROP TABLE IF EXISTS mongodb.joblens.web_notes"      # collection thử của SQLPad
  q "DROP SCHEMA IF EXISTS iceberg.sandbox"
  m 'db.sandbox_jobs.drop()'
  m 'db.getCollection("_schema").deleteOne({table: "sandbox_jobs"})'
  # tin "demo-…" của lệnh kafka: xóa từng dòng (row-level delete của Iceberg) ở snapshot, silver, bronze
  q "DELETE FROM iceberg.silver.job_postings_snapshot WHERE job_uid IN
     (SELECT job_uid FROM iceberg.silver.job_postings WHERE source = 'synthetic' AND source_job_id LIKE 'demo-%')"
  q "DELETE FROM iceberg.silver.job_postings WHERE source = 'synthetic' AND source_job_id LIKE 'demo-%'"
  q "DELETE FROM iceberg.bronze.jobs_raw WHERE source = 'synthetic' AND source_job_id LIKE 'demo-%'"
}

case "${1:-}" in
  sql) sql_demo ;;
  nosql) nosql_demo ;;
  kafka) shift; kafka_demo "$@" ;;
  clean) clean ;;
  *) sed -n '3,14p' "$0"; exit 1 ;;
esac
