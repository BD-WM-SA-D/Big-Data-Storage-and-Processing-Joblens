#!/usr/bin/env bash
# Đưa dữ liệu crawl THẬT của Web Mining vào nền tảng (thí nghiệm F – docs/thi-nghiem-du-lieu-that.md):
#   file JSONL của crawler WM → crawler/producer.py của WM (chạy trên máy này) → Kafka của cụm (port-forward
#   listener `local` 9094) → bronze_jobs (streaming) / bronze_reviews → silver → history → mô hình WM → gold.
# Repo này không chứa code crawler: script dùng mã nguồn WM đúng commit WM_REF (deploy/mining/images.env), clone vào
# ~/.cache/joblens/ như deploy/mining/install.sh. Dữ liệu crawl không commit, không gửi ra ngoài máy và cụm này.
#
#   bash scripts/ingest-crawl.sh <thư mục data/crawl của WM> --dry-run      # chỉ kiểm schema + đếm, không gửi
#   bash scripts/ingest-crawl.sh <thư mục data/crawl của WM>                # chế độ lô: gửi mọi file một lần
#   bash scripts/ingest-crawl.sh <thư mục data/crawl của WM> --stream 30    # chế độ luồng: gửi từng đợt crawl theo
#       thứ tự ngày, cách nhau 30 giây, đo độ trễ tới khi đủ dòng ở bronze (cần bronze_jobs streaming đang chạy:
#       bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds")
# Kết quả (bảng markdown) ghi thêm vào ~/.cache/joblens/ingest-<thời điểm>.md – chép số vào docs, không commit log.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck source=../deploy/mining/images.env
. deploy/mining/images.env

DATA="${1:?Thiếu thư mục data/crawl của Web Mining (nhận từ nhóm WM qua kênh nội bộ)}"; shift
MODE=batch; DELAY=0; DRY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY="--dry-run" ;;
    --stream) MODE=stream; DELAY="${2:-30}"; shift ;;
    *) echo "Tham số lạ: $1"; exit 1 ;;
  esac
  shift
done
[ -d "$DATA" ] || { echo "Không thấy thư mục $DATA"; exit 1; }

CACHE="${XDG_CACHE_HOME:-$HOME/.cache}/joblens"
SRC="$CACHE/web-mining-${WM_REF}"
OUT="$CACHE/ingest-$(date +%Y%m%dT%H%M%S).md"
mkdir -p "$CACHE"
if [ ! -d "$SRC/.git" ]; then git clone -q "$WM_REPO" "$SRC"; fi
git -C "$SRC" fetch -q origin && git -C "$SRC" checkout -q "$WM_REF"
echo "==> Producer của Web Mining @ ${WM_REF} (uv sync --group kafka)"
(cd "$SRC" && uv sync -q --group kafka)

# Thứ tự gửi: tin theo đường dẫn (thư mục đợt crawl đặt tên theo ngày → đúng thứ tự thời gian), rồi review, rồi lỗi parse
mapfile -t JOBS < <(find "$DATA" -name jobs.jsonl | sort)
mapfile -t OTHERS < <(find "$DATA" \( -name reviews.jsonl -o -name parse_errors.jsonl \) | sort)
[ "${#JOBS[@]}" -gt 0 ] || { echo "Không thấy file jobs.jsonl nào trong $DATA"; exit 1; }
echo "    ${#JOBS[@]} file tin, ${#OTHERS[@]} file review/lỗi parse"

produce() {  # produce <file...> – in "đã gửi N → topic"
  (cd "$SRC" && uv run -q python -m crawler.producer "$@" ${DRY:+"$DRY"})
}

if [ -n "$DRY" ]; then
  produce "${JOBS[@]}" "${OTHERS[@]}"
  exit 0
fi

kubectl get pod -n ingest joblens-dual-role-0 >/dev/null
echo "==> Port-forward Kafka (listener local) localhost:9094"
kubectl port-forward -n ingest pod/joblens-dual-role-0 9094:9094 >"$CACHE/pf-kafka.log" 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 20); do (echo >/dev/tcp/127.0.0.1/9094) 2>/dev/null && break; sleep 1; done
export JOBLENS_KAFKA_BOOTSTRAP=localhost:9094

v() { TRINO_FORMAT=CSV_UNQUOTED bash scripts/trino.sh "$1" | tail -1; }
bronze_count() { v "SELECT count(*) FROM iceberg.bronze.jobs_raw WHERE source IN ('topcv', 'itviec')"; }

{
  echo "# Nạp dữ liệu crawl thật – $(date '+%Y-%m-%d %H:%M:%S') – chế độ $MODE"
  echo
  echo "Mã WM: \`$WM_REF\`; thư mục: \`$DATA\`; bronze (topcv+itviec) trước khi gửi: $(bronze_count) dòng"
  echo
} | tee "$OUT"

if [ "$MODE" = batch ]; then
  t0=$(date +%s)
  produce "${JOBS[@]}" "${OTHERS[@]}" | tee -a "$OUT"
  echo "Gửi xong trong $(( $(date +%s) - t0 )) s." | tee -a "$OUT"
else
  if ! kubectl get pods -n compute --no-headers 2>/dev/null | grep -E '^bronze-jobs-' | grep -q Running; then
    echo "Chế độ luồng cần bronze_jobs streaming đang chạy. Mở terminal khác:"
    echo "  bash deploy/spark/submit.sh bronze_jobs --trigger \"10 seconds\""
    exit 1
  fi
  echo "| Đợt crawl (file) | Tin gửi | Trễ tới đủ dòng ở bronze (s) |" | tee -a "$OUT"
  echo "|---|---|---|" | tee -a "$OUT"
  for f in "${JOBS[@]}"; do
    n0=$(bronze_count)
    t0=$(date +%s)
    sent=$(produce "$f" | awk '/jobs\.raw\./ {s += $3} END {print s + 0}')
    lag="quá 600"
    while [ $(( $(date +%s) - t0 )) -lt 600 ]; do
      if [ "$(bronze_count)" -ge $(( n0 + sent )) ]; then lag=$(( $(date +%s) - t0 )); break; fi
      sleep 2
    done
    echo "| \`${f#"$DATA"/}\` | $sent | $lag |" | tee -a "$OUT"
    sleep "$DELAY"
  done
  [ "${#OTHERS[@]}" -eq 0 ] || produce "${OTHERS[@]}" | tee -a "$OUT"
fi

{
  echo
  echo "Bronze (topcv+itviec) sau khi gửi: $(bronze_count) dòng (chế độ lô: bronze tăng khi bronze_jobs chạy)."
} | tee -a "$OUT"
cat <<EOF

Đã ghi: $OUT
Tiếp theo (docs/thi-nghiem-du-lieu-that.md mục 4):
  1. bronze:  EXECUTORS=0 bash deploy/spark/submit.sh bronze_jobs --once   (chế độ lô, khi streaming không chạy)
              EXECUTORS=0 bash deploy/spark/submit.sh bronze_reviews
  2. silver:  EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh silver_job_postings
              EXECUTORS=0 DRIVER_MEMORY=2g bash deploy/spark/submit.sh silver_job_history
  3. đối soát: bash scripts/report-real-ingest.sh
EOF
