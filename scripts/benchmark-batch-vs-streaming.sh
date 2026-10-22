#!/usr/bin/env bash
# Thí nghiệm batch vs streaming (docs/project-plan/03, mục "Batch vs streaming"; ADR docs/adr/0001-lambda-vs-kappa.md).
# Cùng N tin VietJobs đầu tiên đi qua hai đường vào bronze.jobs_raw, rồi đo trên chính dữ liệu trong Iceberg:
#   A – batch:     spark-jobs/vietjobs.py --sink bronze (một job Spark đọc CSV, ghi một lần)
#   B – streaming: replay N tin vào Kafka với RATE tin/giây → bronze_jobs (micro-batch TRIGGER) → bronze
#
# Chỉ số (mỗi đường):
#   độ trễ đầu-cuối  = thời điểm file Parquet chứa dòng được ghi ("$file_modified_time") − crawl_ts của dòng
#                      (A: crawl_ts = lúc job bắt đầu; B: crawl_ts = lúc producer gửi, độ chính xác ms)
#   thông lượng      = số dòng / (file cuối − sự kiện đầu)
#   số file          = số file Parquet mới chứa dữ liệu của lần chạy (file nhỏ → cần compaction)
#   CPU-giây, GB-giờ = cộng dồn "kubectl top" các pod Spark của đường đó, lấy mẫu mỗi 5 giây (ước lượng thô:
#                      metrics-server cập nhật ~15 giây/lần). B còn đo thêm RAM khi job streaming chạy không tải.
#   độ đúng          = số source_job_id có ở đường này mà không có ở đường kia (kỳ vọng 0)
#
# Chạy (Airflow phải tạm dừng để task bảo trì không gộp file giữa chừng – script tự làm):
#   bash scripts/benchmark-batch-vs-streaming.sh [N=5000] [RATE=100] [TRIGGER="10 seconds"]
#   REUSE_A=/tmp/joblens-bench-XXXX bash scripts/…   # dùng lại phần batch của lần chạy trước (cùng N)
# Kết quả in ra cuối (bảng Markdown) và lưu ở thư mục tạm; chép dòng số liệu vào benchmarks/batch_vs_streaming.md
# cùng thông tin máy, rồi thêm một dòng vào deploy/compat.md. Không sửa tay số đo.
set -Eeuo pipefail
# dừng giữa chừng thì in rõ dòng và lệnh hỏng (trước đây script thoát im lặng)
trap 'echo "!! Dừng ở dòng $LINENO: $BASH_COMMAND (mã $?) – log ở ${OUT:-?}" >&2' ERR
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=deploy/lib.sh
. "${ROOT}/deploy/lib.sh"

N="${1:-5000}"
RATE="${2:-100}"
TRIGGER="${3:-10 seconds}"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/joblens-bench-XXXX")"
SAMPLER=""

q() { TRINO_FORMAT=CSV_UNQUOTED bash "${ROOT}/scripts/trino.sh" "$1" | tail -1; }
now_iso() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

start_sampler() {  # start_sampler REGEX FILE – ghi "epoch pod cpu_m mem_Mi" mỗi 5 giây
  # set +e: metrics-server chưa sẵn sàng thì "kubectl top" lỗi – bỏ qua mẫu đó, không để vòng lấy mẫu chết
  ( set +e +o pipefail; trap - ERR
    while true; do
      kubectl top pods -n compute --no-headers 2>/dev/null \
        | awk -v t="$(date +%s)" -v re="$1" '$1 ~ re {gsub("m","",$2); gsub("Mi","",$3); print t, $1, $2, $3}' >> "$2"
      sleep 5
    done ) &
  SAMPLER=$!
}
stop_sampler() {
  if [ -n "$SAMPLER" ]; then kill "$SAMPLER" 2>/dev/null || true; fi
  SAMPLER=""
}
cost() {  # cost FILE → "cpu_s gb_h peak_mem_mi" (mỗi mẫu đại diện 5 giây)
  # không có mẫu nào (metrics-server lỗi suốt lúc chạy) → "–", không ghi 0 giả
  awk '{cpu+=$3*5/1000; mem+=$4*5/3600/1024; t[$1]+=$4} END {if (NR==0) {print "– – –"; exit}
       for (k in t) if (t[k]>p) p=t[k]; printf "%.0f %.4f %.0f\n", cpu, mem, p}' "${1}" 2>/dev/null || echo "– – –"
}
cleanup() { stop_sampler; stop_spark_job bronze-jobs >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> Chuẩn bị: tạm dừng Airflow (không để compaction chạy giữa thí nghiệm), dừng streaming cũ"
bash "${ROOT}/scripts/demo-mode.sh" streaming >/dev/null
stop_spark_job bronze-jobs
echo "    N=${N}, RATE=${RATE} tin/giây, TRIGGER=${TRIGGER}, profile=${PROFILE}, kết quả: ${OUT}"

# ---------------------------------------------------------------- A: batch
if [ -n "${REUSE_A:-}" ]; then
  # dùng lại phần A của một lần chạy trước (cùng N) – tránh chạy lại job batch ~8 phút
  # shellcheck disable=SC1091
  . "${REUSE_A}/state"
  cp "${REUSE_A}/A.samples" "${OUT}/A.samples" 2>/dev/null || : > "${OUT}/A.samples"
  echo "A0=${A0} A_WALL=${A_WALL}" >> "${OUT}/state"
  echo "==> A – dùng lại lần chạy ${REUSE_A} (A0=${A0}, ${A_WALL} giây)"
else
echo "==> A – batch: vietjobs --sink bronze --limit ${N}"
A0=$(now_iso)
start_sampler '^vietjobs-[0-9]' "${OUT}/A.samples"
SECONDS=0
# in log ra màn hình (lọc dòng quan trọng) để thấy job đang chạy; log đầy đủ ở A.log
bash "${ROOT}/deploy/spark/submit.sh" vietjobs --sink bronze --limit "$N" 2>&1 | tee "${OUT}/A.log" \
  | { grep --line-buffered -E "==>|phase: |đọc .* tin|đã ghi|Exception|Error" || true; }
A_WALL=$SECONDS
echo "    A xong sau ${A_WALL} giây"
echo "A0=${A0} A_WALL=${A_WALL}" >> "${OUT}/state"
stop_sampler
fi
read -r A_CPU A_GBH A_PEAK < <(cost "${OUT}/A.samples")

# ---------------------------------------------------------------- B: streaming
echo "==> B – streaming: bronze_jobs --trigger '${TRIGGER}'"
bash "${ROOT}/deploy/spark/submit.sh" bronze_jobs --trigger "$TRIGGER" > "${OUT}/B.submit.log" 2>&1 &
for _ in $(seq 1 60); do
  kubectl get pod -n compute --no-headers 2>/dev/null | grep -q '^bronze-jobs-.* Running' && break; sleep 5
done
echo "    đợi 45 giây cho micro-batch đầu (đọc phần Kafka còn tồn từ checkpoint)"
sleep 45
B0=$(now_iso)
start_sampler '^bronze-jobs-' "${OUT}/B.samples"
echo "==> B – replay ${N} tin, ${RATE} tin/giây"
echo "B0=${B0}" >> "${OUT}/state"
bash "${ROOT}/deploy/data/replay-vietjobs.sh" "$N" "$RATE" 2>&1 | tee "${OUT}/B.replay.log" \
  | { grep --line-buffered -E "==>|đã gửi .*/|lỗi|Error" || true; }
echo "    đợi bronze nhận đủ ${N} dòng (tối đa 10 phút)"
B_WHERE="kafka_topic = 'jobs.raw.vietjobs.v1' AND from_iso8601_timestamp(crawl_ts) >= from_iso8601_timestamp('${B0}')"
for _ in $(seq 1 60); do
  got=$(q "SELECT count(*) FROM iceberg.bronze.jobs_raw WHERE ${B_WHERE}" || echo 0)
  echo "    bronze đã có ${got:-0}/${N} dòng"
  [ "${got:-0}" -ge "$N" ] && break; sleep 10
done
stop_sampler
read -r B_CPU B_GBH B_PEAK < <(cost "${OUT}/B.samples")
echo "==> B – đo RAM khi job streaming chạy không tải (60 giây)"
start_sampler '^bronze-jobs-' "${OUT}/B.idle.samples"; sleep 60; stop_sampler
read -r _ B_IDLE_GBH B_IDLE_PEAK < <(cost "${OUT}/B.idle.samples")
stop_spark_job bronze-jobs

# ---------------------------------------------------------------- số đo trên dữ liệu
A_WHERE="kafka_topic = 'backfill.vietjobs' AND from_iso8601_timestamp(crawl_ts) >= from_iso8601_timestamp('${A0}')"
metrics() {  # metrics WHERE → rows,ids,files,p50_s,p95_s,max_s,throughput
  q "SELECT count(*), count(DISTINCT source_job_id), count(DISTINCT \"\$path\"),
       round(approx_percentile(date_diff('millisecond', from_iso8601_timestamp(crawl_ts), \"\$file_modified_time\"), 0.5) / 1e3, 1),
       round(approx_percentile(date_diff('millisecond', from_iso8601_timestamp(crawl_ts), \"\$file_modified_time\"), 0.95) / 1e3, 1),
       round(max(date_diff('millisecond', from_iso8601_timestamp(crawl_ts), \"\$file_modified_time\")) / 1e3, 1),
       round(count(*) / greatest(date_diff('millisecond', min(from_iso8601_timestamp(crawl_ts)), max(\"\$file_modified_time\")) / 1e3, 0.001), 1)
     FROM iceberg.bronze.jobs_raw WHERE $1"
}
IFS=, read -r A_ROWS A_IDS A_FILES A_P50 A_P95 A_MAX A_TPUT < <(metrics "$A_WHERE")
IFS=, read -r B_ROWS B_IDS B_FILES B_P50 B_P95 B_MAX B_TPUT < <(metrics "$B_WHERE")
DIFF=$(q "SELECT count(*) FROM (SELECT DISTINCT source_job_id AS id FROM iceberg.bronze.jobs_raw WHERE ${A_WHERE}) a
          FULL OUTER JOIN (SELECT DISTINCT source_job_id AS id FROM iceberg.bronze.jobs_raw WHERE ${B_WHERE}) b
          ON a.id = b.id WHERE a.id IS NULL OR b.id IS NULL")

cat > "${OUT}/result.md" <<EOF
<!-- benchmark-batch-vs-streaming.sh $(date -u +%FT%TZ) – commit $(git -C "$ROOT" rev-parse --short HEAD), profile ${PROFILE} -->
Tham số: N=${N}, RATE=${RATE} tin/giây, TRIGGER=${TRIGGER}

| Đường | Dòng | ID khác nhau | File mới | Trễ p50 (s) | Trễ p95 (s) | Trễ max (s) | Thông lượng (dòng/s) | CPU-giây | GB-giờ | RAM đỉnh (MiB) | Thời gian job (s) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A – batch | ${A_ROWS} | ${A_IDS} | ${A_FILES} | ${A_P50} | ${A_P95} | ${A_MAX} | ${A_TPUT} | ${A_CPU} | ${A_GBH} | ${A_PEAK} | ${A_WALL} |
| B – streaming | ${B_ROWS} | ${B_IDS} | ${B_FILES} | ${B_P50} | ${B_P95} | ${B_MAX} | ${B_TPUT} | ${B_CPU} | ${B_GBH} | ${B_PEAK} | – |

- Độ đúng: ${DIFF} source_job_id chỉ có ở một trong hai đường (kỳ vọng 0).
- Streaming khi không tải: RAM đỉnh ${B_IDLE_PEAK} MiB, ${B_IDLE_GBH} GB-giờ trong 60 giây (chi phí thường trực của đường B).
- Thời gian job A gồm cả build/nạp image (nếu image đã có thì gần như 0) và khởi động pod.
EOF
echo
cat "${OUT}/result.md"
echo
echo "Log và mẫu đo: ${OUT}. Bật lại Airflow khi cần: bash scripts/demo-mode.sh airflow"
