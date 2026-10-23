# Bài học kinh nghiệm (nguồn cho mục 4 báo cáo IT4931)

Mẫu theo yêu cầu môn học: **Mô tả vấn đề → Các cách đã thử (kèm đánh đổi) → Giải pháp cuối cùng → Điều rút ra.**
Mỗi bài học là sự việc thật đã gặp khi dựng nền tảng (10/2026), kèm ngày và file liên quan để kiểm chứng. Số liệu
lấy từ log/đầu ra thật; chỗ chưa đo ghi `<cần đo>`. Khi chuyển vào báo cáo, viết lại thành văn (quy tắc R3: không chép
nguyên văn giữa các báo cáo).

---

## 1. Thu thập dữ liệu (nguồn đa dạng, chất lượng, dữ liệu trễ, trùng lặp, phiên bản)

### 1.1. "0.0 triệu" nghĩa là lương thỏa thuận (2026-10-19)

- **Vấn đề:** sau khi nạp 48.092 tin VietJobs, tỉ lệ tin "thỏa thuận" ở gold bằng 0 – trái với thực tế thị trường.
  Xem dữ liệu gốc: tin thỏa thuận được ghi thành `"0.0 triệu"`, bộ chuẩn hóa hiểu là lương 0 rồi loại vì ngoài khoảng hợp lệ.
- **Đã thử:** (a) coi mọi lương 0 là thiếu – mất thông tin "thỏa thuận"; (b) sửa ở dbt – logic chuẩn hóa nằm hai nơi.
- **Giải pháp:** quy tắc trong `spark-jobs/normalize.py`: chuỗi lương toàn số 0 → `salary_is_negotiable = true`; thêm ca
  kiểm thử `("0.0 triệu", …)` trong `tests/test_normalize.py`. Tỉ lệ thỏa thuận theo cấp bậc sau sửa: 0,2–0,5.
- **Điều rút ra:** mỗi nguồn có quy ước riêng – phải đọc mẫu dữ liệu gốc trước khi viết quy tắc, và giữ một nơi duy nhất
  cho logic chuẩn hóa.

### 1.2. Địa điểm ghi theo quận/huyện, thành phố cũ (2026-10-19 → 10-20)

- **Vấn đề:** chỉ 93,7% tin ánh xạ được về 34 tỉnh sau sắp xếp 2025; phần còn lại là "bắc từ liêm", "quận 7", "tp vinh"…
- **Đã thử:** (a) so khớp mờ (fuzzy) – dễ nhầm "vinh" với "vĩnh long"; (b) danh sách alias theo dữ liệu thật.
- **Giải pháp:** truy vấn `scripts/check-locations.sql` liệt kê chuỗi chưa ánh xạ nhiều nhất → thêm alias vào
  `dbt/seeds/locations_2025.csv` (một file dùng chung cho Spark và dbt), kiểm thử "vĩnh long" không nhầm "vinh".
  Kết quả: 0,937 → 0,985; còn lại chủ yếu là địa điểm nước ngoài (san francisco, new york) – cố ý để trống.
- **Điều rút ra:** đo độ phủ trước, sửa theo tần suất (vài chục alias phủ gần hết phần thiếu); quy tắc tường minh dễ
  kiểm thử hơn so khớp mờ.

### 1.3. Trùng lặp khi một tin đến qua hai đường

- **Vấn đề:** cùng 5.000 tin vào bronze qua batch (`backfill.vietjobs`) và streaming (`jobs.raw.vietjobs.v1`); bronze có
  thêm hàng chục nghìn dòng trùng sau các lần thí nghiệm.
- **Đã thử:** khử trùng ở bronze – mất tính append-only, khó tái lập.
- **Giải pháp:** bronze giữ nguyên mọi bản ghi; silver `MERGE INTO` theo `job_uid = sha1(source:source_job_id)`, lấy bản
  crawl mới nhất. Kiểm chứng: GX `expect_column_values_to_be_unique(job_uid)` đạt, silver 48.095 dòng (48.092 + 3 tin mẫu)
  dù bronze nhiều hơn nhiều.
- **Điều rút ra:** chọn khóa tự nhiên ổn định sớm; khử trùng ở tầng có ngữ nghĩa (silver), không ở tầng thô.

### 1.4. Dữ liệu đến trễ

- **Vấn đề:** crawler có thể gửi tin với `crawl_ts` cũ (mạng chậm, chạy lại). Đếm theo thời điểm nhận sẽ sai cửa sổ.
- **Đã thử:** đếm theo `ingest_ts` – đơn giản nhưng sai ngữ nghĩa; không giới hạn độ trễ – state lớn mãi.
- **Giải pháp:** `stream_province_windows.py` dùng event time `crawl_ts` + watermark 10 phút; tin trễ hơn bị bỏ và được đếm
  (`numRowsDroppedByWatermark`). Kiểm thử trên luồng thật (file source) trong `tests/test_stream_province_windows.py`.
  Trên cụm: `<cần đo>` với `replay-vietjobs.sh --late-every 10`.
- **Điều rút ra:** watermark là đánh đổi giữa độ đầy đủ và kích thước state – phải chọn theo độ trễ thực tế của nguồn.

## 2. Xử lý dữ liệu với Spark (tối ưu job, bộ nhớ, partition)

### 2.1. Image Spark dùng Python 3.10, máy dev dùng 3.12

- **Vấn đề:** job chạy được ở máy dev nhưng lỗi trong cụm: `datetime.UTC` chỉ có từ Python 3.11.
- **Giải pháp:** dùng `timezone.utc`; khi chọn thư viện cho image (pymongo, pyarrow), giải phụ thuộc cho đúng Python 3.10
  (`uv pip compile --python-version 3.10`) – vì vậy image pin `pymongo 4.18.3`, `pyarrow 25.0.1` khác bản trong `uv.lock`.
- **Điều rút ra:** kiểm thử phải chạy trên cùng phiên bản runtime với cụm, hoặc ít nhất giải phụ thuộc theo runtime đó.

### 2.2. Không trộn được pandas UDF tổng hợp với hàm có sẵn (2026-10-23, khi viết job phân tích)

- **Vấn đề:** Spark không cho trộn pandas UDF tổng hợp với hàm tổng hợp thường trong cùng một `agg`
  (`groupBy().agg(countDistinct(...), trimmed_mean(...))`). Ngoài ra khai báo kiểu trả về bằng chuỗi `"double"` làm import
  module lỗi `SESSION_OR_CONTEXT_NOT_EXISTS` khi chưa có SparkSession (gặp trong kiểm thử).
- **Giải pháp:** tính hai phần riêng rồi join theo khóa nhóm (`analytics_spark.salary_by_province`); khai báo kiểu trả về
  bằng `DoubleType()` thay vì chuỗi `"double"` để import module không cần SparkSession.
- **Điều rút ra:** pandas UDF chạy theo đường thực thi riêng (Arrow) – thiết kế phép tổng hợp theo giới hạn đó.

### 2.3. Bộ nhớ: driver + executor trên máy 16 GB

- **Vấn đề:** mỗi job Spark cluster mode cần driver + executor (~1,4 GB mỗi pod với 1 GB heap + overhead 40% cho PySpark).
- **Giải pháp:** profile `lite` chạy local mode (`EXECUTORS=0`, một pod) cho máy 8 GB; DAG xếp các task Spark tuần tự.
- **Điều rút ra:** overhead bộ nhớ của PySpark (tiến trình Python cạnh JVM) đáng kể – phải tính vào khi đặt limit pod.

## 3. Xử lý luồng (exactly-once, cửa sổ, state, phục hồi)

### 3.1. Micro-batch chạy lâu hơn chu kỳ trigger (2026-10-23)

- **Vấn đề:** thí nghiệm lần 1 (trigger 10 giây, 100 tin/giây, 5.000 tin): streaming chỉ ghi 2 file trong ~50 giây gửi;
  độ trễ p50 46,2 giây – mỗi micro-batch có dữ liệu chạy lâu hơn 10 giây.
- **Đã thử:** `<cần đo>`: xem *Batch Duration* trên Spark UI; giảm số lần quét trong `foreachBatch` (hiện `persist` + 2 lần
  `count` + ghi); tăng executor.
- **Giải pháp tạm:** chấp nhận – p95 60,6 giây vẫn đạt chỉ tiêu ≤ 2 phút.
- **Điều rút ra:** trigger chỉ là chu kỳ tối thiểu; độ trễ thật = thời gian xử lý micro-batch. Đo trước khi chọn trigger.

### 3.2. Ghi idempotent cho sink ngoài Spark

- **Vấn đề:** `foreachBatch` có thể chạy lại cùng micro-batch sau sự cố → sink cộng dồn sẽ đếm hai lần.
- **Giải pháp:** ghi giá trị tuyệt đối của cửa sổ (output mode `update`) và upsert theo `_id = "<đầu cửa sổ>|<mã tỉnh>"`.
  Bronze (Iceberg): commit theo micro-batch + checkpoint offset.
- **Điều rút ra:** exactly-once đầu-cuối = nguồn phát lại được + checkpoint + sink idempotent; thiếu một trong ba là mất.

### 3.3. Chi phí thường trực của streaming

- **Vấn đề:** job streaming giữ ~1,4 GB RAM kể cả khi không có tin (đo 1.377 MiB, 2026-10-23).
- **Giải pháp:** trên máy dev tắt streaming khi không demo (`demo-mode.sh airflow`); ghi vào ADR như chi phí của Kappa.
- **Điều rút ra:** so sánh batch/streaming phải tính cả chi phí cố định, không chỉ chi phí theo lượng dữ liệu.

## 4. Lưu trữ (định dạng, phân vùng, nén, dữ liệu nóng/lạnh)

### 4.1. File nhỏ do streaming

- **Vấn đề:** mỗi micro-batch sinh ít nhất một file Parquet mỗi phân vùng; truy vấn phải mở nhiều file.
- **Giải pháp:** task `iceberg_maintenance` (`rewrite_data_files` binpack 128 MB, `rewrite_manifests`, `expire_snapshots`
  giữ 10 bản) chạy trong DAG; Grafana theo dõi số file và kích thước trung bình. Không chạy `remove_orphan_files` khi
  streaming đang ghi (có thể xóa file chưa commit). Số file trước/sau: `<cần đo>`.
- **Điều rút ra:** lakehouse cần bảo trì định kỳ như cơ sở dữ liệu; compaction an toàn với ghi đồng thời nhờ snapshot.

### 4.2. Kiểu dữ liệu thời gian khác nhau giữa các tầng (2026-10-23)

- **Vấn đề:** script thí nghiệm gọi `from_iso8601_timestamp(crawl_ts)` → Trino báo lỗi: `crawl_ts` trong bronze đã là
  `TIMESTAMP` (Spark parse khi nạp), chỉ trong message Kafka mới là chuỗi.
- **Giải pháp:** dùng thẳng cột; mốc thời gian dạng chuỗi của script mới cần parse.
- **Điều rút ra:** đọc contract bảng (`joblens/schemas/tables/bronze/jobs_raw.yaml`) thay vì đoán theo message.

## 5. Tích hợp hệ thống (khám phá dịch vụ, xử lý lỗi, …)

### 5.1. Helm hook kẹt với `--wait` (Lakekeeper 2026-10-17; áp dụng trước cho Airflow 2026-10-20)

- **Vấn đề:** cài Lakekeeper, `helm upgrade --install --wait` treo tới hết thời gian chờ: pod chờ job migrate DB, còn job
  migrate là hook post-install chỉ chạy sau khi helm hết chờ → vòng chờ. Airflow có cấu trúc tương tự nên cấu hình sẵn.
- **Giải pháp:** Lakekeeper `helmWait: true`; Airflow `migrateDatabaseJob.useHelmHooks: false` và xóa job cũ trước mỗi
  lần nâng cấp (spec Job bất biến).
- **Điều rút ra:** hiểu vòng đời Helm (hook vs tài nguyên thường) trước khi dùng `--wait` trong script tự động.

### 5.1b. MongoDB "sẵn sàng" nhưng từ chối kết nối (2026-10-23)

- **Vấn đề:** lần cài đầu, tạo user ứng dụng lỗi `ECONNREFUSED 127.0.0.1:27017` dù readiness probe đã đạt. Image mongo
  chạy một mongod tạm để tạo user root rồi tự khởi động lại – probe đạt đúng trong pha tạm đó.
- **Giải pháp:** script thử lại 12 lần × 5 giây; chạy lại lần hai thì thành công.
- **Điều rút ra:** "ready" của Kubernetes chỉ đúng bằng probe; bước khởi tạo phải tự chịu lỗi tạm thời (retry).

### 5.2. Bộ lọc thời gian của Grafana–Trino lệch múi giờ (2026-10-20)

- **Vấn đề:** ô "tin nạp theo 10 giây" báo *No data* dù bronze tăng. Plugin Trino chèn mốc thời gian dạng `TIMESTAMP '…'`
  không kèm múi giờ.
- **Giải pháp:** lọc theo epoch giây (`$__unixEpochFilter(to_unixtime(...))`) – không phụ thuộc múi giờ phiên.
- **Điều rút ra:** luôn so sánh thời gian bằng giá trị có múi giờ hoặc epoch khi đi qua nhiều hệ thống.

### 5.3. Phiên bản chart không tồn tại

- **Vấn đề:** SeaweedFS chart 4.48.2 (đúng bản ứng dụng) chưa được phát hành; cài lỗi.
- **Giải pháp:** `helm search repo seaweedfs --versions` → pin 4.48.0; ghi vào `deploy/compat.md`.
- **Điều rút ra:** pin phiên bản *chart* và *ứng dụng* riêng, kiểm tra bản có thật trước khi ghi vào tài liệu.

## 6. Tối ưu hiệu năng (cache, truy vấn, cấp phát tài nguyên, nút thắt)

### 6.0. Trino khởi động lại giữa lần dbt build (2026-10-23 01:47)

- **Vấn đề:** `dbt_build_gold` lỗi "Connection refused" tới Trino khi seed và mart chạy song song (dbt 2 luồng); pod
  coordinator bắt đầu lại lúc 01:49. Nguyên nhân không còn xem được (Kubernetes chỉ giữ lần khởi động lại gần nhất), khả
  năng cao là vượt limit bộ nhớ: limit 1792Mi chỉ hơn heap 1400M khoảng 0,4 GB.
- **Giải pháp:** limit 2304Mi (chừa ~0,9 GB ngoài heap); dbt trong cụm chạy 1 luồng. Lần chạy sau (21:41): dbt xanh.
- **Điều rút ra:** limit container của ứng dụng JVM = heap + bộ nhớ ngoài heap; ghi nhận nguyên nhân ngay khi lỗi
  (`kubectl describe pod … | grep -A4 "Last State"`) trước khi khởi động lại cụm.

### 6.1. Nạp image vào k3d mất vài phút mỗi lần chạy job

- **Vấn đề:** "thời gian job" batch 468 giây chủ yếu là `k3d image import` image Spark ~2 GB vào từng node (lần đo
  2026-10-23); mỗi `submit.sh` đều nạp lại.
- **Giải pháp:** `deploy/lib.sh build_import` so ID image trên từng node (crictl), trùng thì bỏ qua. Hiệu quả: `<cần đo>`.
- **Điều rút ra:** đo từng bước của "thời gian chạy" trước khi tối ưu – nút thắt nằm ở triển khai, không phải Spark.

### 6.2. Broadcast join cho bảng tỉnh

- **Vấn đề:** join tin (48 nghìn dòng, sau explode nhiều hơn) với 34 tỉnh; dữ liệu lệch – Hà Nội 21.929 và TP.HCM 18.332 tin.
- **Giải pháp:** `F.broadcast(dim)` → `BroadcastHashJoin`, không shuffle bảng lớn, không bị partition lệch; kiểm tra bằng
  plan (`plan_checks.broadcast_join`).
- **Điều rút ra:** bảng nhỏ + khóa lệch → broadcast; hai bảng lớn → sort-merge (AQE tách partition lệch).

## 7. Giám sát và gỡ lỗi (metrics, cảnh báo, log, tìm nguyên nhân gốc)

### 7.1. Script thí nghiệm thoát im lặng (2026-10-23)

- **Vấn đề:** script benchmark dừng sau bước batch, không báo lỗi. Hai lần chạy mới tìm ra.
- **Đã thử:** in tiến độ ra màn hình; thêm `trap ERR` in dòng lỗi → lộ ra `kill "$SAMPLER"` thất bại.
- **Giải pháp:** vòng lấy mẫu `kubectl top` (subshell) kế thừa `set -e`: metrics-server vừa khởi động lại làm một lệnh
  lỗi → vòng chết → `kill` thất bại → cả script dừng. Tắt errexit trong vòng lấy mẫu; không có mẫu thì ghi "–", không ghi 0.
- **Điều rút ra:** script tự động phải báo *ở đâu* nó dừng; số đo thiếu phải hiện là thiếu, không phải 0.

### 7.2. Không đọc được log task trên giao diện Airflow (2026-10-23)

- **Vấn đề:** "Could not read served logs … Read timed out" khi scheduler bận.
- **Giải pháp:** đọc trực tiếp: `kubectl logs` pod task (pod lỗi được giữ lại), hoặc file log trong pod scheduler.
- **Điều rút ra:** biết đường dẫn log ở mọi tầng (pod, file, giao diện); giao diện chỉ là một cách xem.

## 8. Mở rộng (ngang vs dọc, tự động, dự trù tài nguyên, chi phí)

### 8.1. Từ máy 16 GB xuống 8 GB (2026-10-20 → 10-22)

- **Vấn đề:** WSL 10 GB, còn trống 2 GB khi bật đủ thành phần; Grafana không lên.
- **Đã thử:** tăng RAM cho WSL (12 GB) – vẫn thiếu khi streaming + DAG cùng lúc.
- **Giải pháp:** đo bằng `kubectl top` (cả cụm ~5,3 GB nhàn rỗi sau tối ưu: Trino 771 MiB, Kafka 538 MiB) → Trino một node,
  Kafka heap 512 MB, replay bằng producer Python thay vì Spark, chế độ demo không chạy streaming và DAG cùng lúc. Profile
  `lite` cho máy 8 GB (ước tính 3–3,5 GB nhàn rỗi, `<cần đo>`); máy 4 GB: không đủ, dùng Codespaces.
- **Điều rút ra:** dự trù theo số đo, không theo bảng ước lượng; giảm cụm theo chiều dọc (bớt node, bớt heap) trước,
  mở rộng ngang (thêm Trino worker, executor) khi có máy lớn hơn – cấu hình đã tách theo profile.

## 9. Chất lượng dữ liệu và kiểm thử

### 9.1. Cổng chất lượng trước gold (2026-10-23)

- **Vấn đề:** dbt test kiểm gold, nhưng dữ liệu silver sai (mã tỉnh lạ, lương max < min) vẫn đi vào gold.
- **Giải pháp:** task GX `gx_validate_silver` với 15 kỳ vọng (khóa, danh mục, khoảng lương, mã tỉnh, độ phủ địa điểm ≥ 90%,
  lương ≥ 50%); hỏng thì dbt không chạy. Lần chạy đầu: 15/15 đạt, 1,55% tin thiếu địa điểm, 28,55% thiếu lương.
- **Điều rút ra:** kiểm tra theo tầng (GX ở silver, dbt test ở gold, pytest cho mã); ngưỡng độ phủ đặt theo số đo thật.

### 9.2. Kiểm thử không cần cụm

- **Giải pháp:** pytest với Spark local cho mọi job (kể cả watermark trên luồng file thật); script shell kiểm thử bằng
  `kubectl` giả; DAG kiểm tra bằng `DagBag` của Airflow 3.3.2; giá trị Helm kiểm tra bằng `values.schema.json` của chart.
- **Điều rút ra:** phần lớn lỗi bắt được trước khi chạm cụm; phần còn lại (múi giờ, kiểu dữ liệu thật, bộ nhớ) chỉ lộ
  trên cụm – cần cả hai.

## 10. Bảo mật và quản trị (truy cập, mã hóa, nhật ký, tuân thủ)

### 10.1. Bí mật không vào git

- **Giải pháp:** khóa S3, mật khẩu Postgres/MongoDB/Airflow/Grafana sinh ngẫu nhiên vào Secret của cụm hoặc `.env`
  (gitignore); pre-commit chặn `.env` và quét bằng gitleaks; Airflow Variables khai báo qua Helm values.
- **Rủi ro còn lại:** môi trường dev không bật xác thực Trino/Lakekeeper (chỉ mở qua port-forward); script in mật khẩu ra
  terminal – không chia sẻ ảnh chụp màn hình đó.
- **Điều rút ra:** tách "bí mật" khỏi "cấu hình" ngay từ đầu rẻ hơn nhiều so với làm sạch lịch sử git sau này.

### 10.2. Quyền tối thiểu

- **Giải pháp:** MongoDB user `joblens` chỉ `readWrite` trên database `joblens`; Kafka UI chế độ chỉ đọc (topic chỉ tạo
  bằng `KafkaTopic`); Airflow chỉ được tạo pod trong namespace `compute` (`deploy/airflow/rbac.yaml`).
- **Điều rút ra:** mỗi thành phần một quyền vừa đủ; giao diện quản trị dễ bị dùng để "sửa nhanh" ngoài manifest.

## 11. Chịu lỗi (phục hồi, nhân bản, sao lưu, khôi phục thảm họa)

### 11.1. Cụm k3d không lên sau khi tắt WSL (2026-10-20)

- **Vấn đề:** `kubectl` báo *connection reset by peer*. Log: `NodeIPs changed … 172.18.0.2 → 172.18.0.3` – node đổi IP,
  load balancer của k3d vẫn trỏ IP cũ.
- **Giải pháp:** `k3d cluster stop joblens && k3d cluster start joblens`; đưa vào mục sự cố của `docs/demo.md`.
- **Điều rút ra:** địa chỉ IP trong mạng Docker không ổn định qua lần khởi động – dùng tên dịch vụ, và có quy trình
  khởi động lại đã thử trước.

### 11.2. Task Airflow bị giết khi tắt scheduler (2026-10-23)

- **Vấn đề:** để giải phóng RAM, script tắt scheduler trong lúc task đang chạy → task bị đánh dấu lỗi, các task sau bị bỏ.
- **Giải pháp:** `demo-mode.sh streaming` tạm dừng DAG, đợi pod task kết thúc rồi mới tắt; DAG có `retries=1` (lượt chạy
  thủ công sau đó tự chạy lại `silver_job_postings` thành công).
- **Điều rút ra:** dừng hệ thống phải "êm" (drain) – dừng nhận việc mới, đợi việc đang làm xong.

### 11.3. Kafka khởi động lại nhiều lần sau khi tắt máy đột ngột (2026-10-22)

- **Vấn đề:** pod Kafka `CrashLoopBackOff` sau khi máy tắt không dừng cụm.
- **Giải pháp:** tự hồi phục sau vài phút (không xem log nguyên nhân – `<cần bổ sung nếu gặp lại>`); dữ liệu nằm trên
  volume nên không mất. Quy trình tắt máy: `k3d cluster stop joblens` trước.
- **Điều rút ra:** dịch vụ có trạng thái cần dừng đúng cách; một broker = không có nhân bản – chấp nhận ở môi trường dev,
  ghi rõ trong báo cáo.
