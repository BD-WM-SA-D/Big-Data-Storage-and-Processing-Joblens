# Tập dượt demo – từng bước, từng terminal, kết quả mong đợi

Dùng để tập dượt checkpoint 26/10 và buổi demo cuối kỳ, và để thành viên khác tự chạy thử trên máy mình. Kịch bản
trình bày (nói gì, cho xem gì): [`demo.md`](demo.md). Kiến trúc và dữ liệu: [`kien-truc.md`](kien-truc.md).

Thời gian: lần đầu ~60 phút (gồm chờ cụm khởi động và DAG chạy), các lần sau ~35 phút.

## Quy ước

- **Mọi lệnh chạy trong terminal Ubuntu (WSL)**, trừ chỗ ghi rõ *PowerShell*. Trên Windows Terminal: mở tab Ubuntu mới
  bằng nút **˅** cạnh dấu **+** → *Ubuntu* (hoặc `Ctrl+Shift+T` nếu Ubuntu là hồ sơ mặc định).
- Mỗi terminal mới phải `cd` vào repo trước:
  ```bash
  cd ~/joblens/Big-Data-Storage-and-Processing-joblens
  ```
- **Trình duyệt mở trên Windows** (Chrome/Edge), địa chỉ `http://localhost:…` dùng được trực tiếp.
- Ký hiệu: 🟦 **T1**, 🟩 **T2**, 🟨 **T3**, 🟥 **T4** = terminal Ubuntu số 1–4. ✅ = đầu ra nghĩa là thành công.
- Không dán mật khẩu (Grafana, Airflow, MongoDB) vào nhóm chat hay ảnh chụp màn hình.

Máy mới chưa có cụm: làm mục 8 trước. Không đủ RAM (dưới 16 GB): dùng `JOBLENS_PROFILE=lite` hoặc GitHub
Codespaces ([`cloud-codespaces.md`](cloud-codespaces.md)).

---

## 1. Khởi động (🟦 T1)

1. Mở **Docker Desktop** trên Windows, đợi góc dưới báo *Engine running*.
2. Mở terminal Ubuntu **T1**:
   ```bash
   cd ~/joblens/Big-Data-Storage-and-Processing-joblens
   git pull
   uv sync
   k3d cluster start joblens
   ```
   ✅ `git pull` báo `Already up to date.` hoặc danh sách file mới; dòng cuối của k3d: `Started cluster 'joblens'`.
3. Đợi 3–5 phút rồi kiểm tra pod:
   ```bash
   kubectl get pods -A | grep -v -E "Running|Completed"
   ```
   ✅ Chỉ còn dòng tiêu đề `NAMESPACE NAME READY STATUS …` (mọi pod đã Running/Completed).
   Chạy lại lệnh sau 1–2 phút nếu còn pod `Pending`, `Init`, `ContainerCreating`.

   | Thấy | Nghĩa là | Làm gì |
   |---|---|---|
   | `connection refused` / `connection reset` | Cụm khởi động dở sau khi tắt máy | `k3d cluster stop joblens && k3d cluster start joblens` |
   | Pod `Error` hoặc `Terminating` tuổi hàng giờ trong `compute` | Pod task cũ bị cắt khi tắt cụm, vô hại | `kubectl delete pod -n compute <tên>` (thêm `--force --grace-period=0` nếu kẹt) |
   | `trino-coordinator` `Pending` quá 5 phút | Thiếu RAM | `bash scripts/demo-mode.sh status`; đóng bớt ứng dụng Windows |

4. (Tùy chọn, ~2 phút) kiểm thử mã:
   ```bash
   uv run pytest -q
   ```
   ✅ Dòng cuối dạng `161 passed, 1 skipped` – quan trọng là **không có `failed`**.

## 2. Mở giao diện (🟦 T1 – để nguyên suốt buổi)

```bash
bash scripts/demo-ui.sh
```

✅ In ra danh sách và **không trở về dấu nhắc lệnh** (đúng như vậy – đừng tắt). Profile `lite` không có Kafka UI:

```
Giao diện demo (Ctrl+C để đóng):
  Grafana      http://localhost:3000/d/joblens-demo
  Kafka UI     http://localhost:8081/ui/clusters/joblens/all-topics
  Airflow      http://localhost:8082/dags/joblens_pipeline
  Trino        http://localhost:8080/ui/
  Lakekeeper   http://localhost:8181/ui/
  SeaweedFS    http://localhost:8888/buckets/joblens/
```

Mở từng địa chỉ trên trình duyệt. ✅ Grafana hiện dashboard *JobLens – Big Data demo*, các ô số ở hàng đầu có giá
trị (silver ~48,1K, bronze ~74K). Grafana trắng hoặc *Unable to find application file*: đợi 1 phút rồi tải lại trang.

Mật khẩu Airflow (chạy ở **T2**, đừng chụp màn hình):
```bash
kubectl get secret airflow-admin -n orchestration -o jsonpath='{.data.password}' | base64 -d; echo
```

## 3. Kiến trúc và dữ liệu (🟩 T2 – mở terminal mới)

1. Trình duyệt: GitHub → repo → `docs/kien-truc.md`. ✅ Hai sơ đồ (luồng dữ liệu, triển khai K8s) hiện thành hình.
2. **T2:**
   ```bash
   kubectl get pods -A
   ```
   ✅ Thấy các namespace `ingest`, `lakehouse`, `compute`, `orchestration`, `serving`, `monitoring`.
3. **T2:**
   ```bash
   bash scripts/trino.sh -f scripts/demo-queries.sql
   ```
   ✅ 7 bảng kết quả in liên tiếp, không có dòng `Query … failed`. Bảng đầu có `vietjobs | backfill.vietjobs` ~48 nghìn
   dòng; bảng 2 có `ti_le_co_dia_diem` ~0,985.
4. Trình duyệt SeaweedFS → `warehouse/` → bảng `jobs_raw` → `data/` (file `.parquet` theo phân vùng) và `metadata/`.

## 4. Streaming (🟩 T2, 🟨 T3, 🟥 T4)

1. **T2** – tắt Airflow tạm thời để có RAM:
   ```bash
   bash scripts/demo-mode.sh streaming
   ```
   ✅ Dòng cuối: `Giờ chạy: bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds" …`.
   Nếu báo `còn N task Airflow đang chạy – đợi xong`: cứ đợi, script tự tiếp tục.

2. **T2** – job streaming có trạng thái (cửa sổ 5 phút theo tỉnh → MongoDB). Lệnh **chạy mãi**, để nguyên:
   ```bash
   DRIVER_MEMORY=768m EXECUTORS=0 bash deploy/spark/submit.sh stream_province_windows
   ```
   ✅ Sau 1–2 phút có dòng `bắt đầu: cửa sổ 5 minutes, watermark 10 minutes, output mode update …`, sau đó lặp lại
   `Streaming query has been idle and waiting for new data …`.

3. **🟨 T3 – mở terminal mới** – job streaming nạp bronze, micro-batch 10 giây. **Chạy mãi**, để nguyên:
   ```bash
   cd ~/joblens/Big-Data-Storage-and-Processing-joblens
   DRIVER_MEMORY=768m EXECUTOR_MEMORY=768m bash deploy/spark/submit.sh bronze_jobs --trigger "10 seconds"
   ```
   ✅ Sau 1–2 phút log lặp lại `Streaming query has been idle and waiting for new data more than 10000 ms.`

4. **🟥 T4 – mở terminal mới** – chỉ chạy khi **cả T2 và T3 đã báo `idle`** (job chỉ đọc tin gửi sau lúc nó chạy):
   ```bash
   cd ~/joblens/Big-Data-Storage-and-Processing-joblens
   bash deploy/data/replay-vietjobs.sh 1000 50 --late-every 10
   ```
   ✅ `replay: đã gửi 1000/1000 tin vào jobs.raw.vietjobs.v1` rồi `==> Xong` (~30 giây).

5. Grafana: chọn **Last 15 minutes**, làm mới **5s**. ✅ Trong 10–30 giây: ô *Tin nạp vào bronze theo 10 giây* có cột
   mới; ô *Tin mới theo tỉnh – cửa sổ 5 phút* có cột màu theo tỉnh; *Dòng trong bronze* tăng thêm 1.000.
   Kafka UI → *Topics* → `jobs.raw.vietjobs.v1` → *Messages*: thấy message JSON.

6. **T4** – số đo watermark (log T2 bị Spark che, nên lọc ra):
   ```bash
   POD=$(kubectl get pods -n compute --no-headers | grep province | grep Running | awk '{print $1}')
   kubectl logs -n compute "$POD" | grep -E "tiến độ batch|cửa sổ×tỉnh" | tail -15
   ```
   ✅ Các dòng `batch N: … cửa sổ×tỉnh (mới …, cập nhật …)` và
   `tiến độ batch N: vào … dòng; state … dòng; bỏ do trễ (watermark) K; watermark 2026-…`. Có batch với K > 0 = tin trễ
   đã bị watermark loại. **Ghi lại các số này** (quy tắc: chỉ nói số đo thật).

**Giữ T2 và T3 chạy** cho mục 5 (lệnh `kafka` dùng job bronze đang chạy).

## 5. Thêm / sửa / xóa dữ liệu tại chỗ (🟥 T4)

Mỗi lệnh in câu lệnh màu vàng (`trino>` / `mongo>`) rồi dừng ở `… Enter để tiếp` – bấm **Enter** để sang bước sau.

1. SQL trên Iceberg:
   ```bash
   bash scripts/demo-data.sh sql
   ```
   ✅ Lần lượt thấy `CREATE SCHEMA`, `CREATE TABLE: 5 rows`, bảng 5 tin; `INSERT: 1 row`; `UPDATE: 1 row` và
   `salary_max_vnd` đổi thành `35000000`; `truoc_khi_xoa` > `sau_khi_xoa`; bảng snapshot có các dòng `append`,
   `overwrite`/`delete`; `so_tin_luc_tao_bang` = **5** (time travel) khác `so_tin_hien_tai`.

2. NoSQL trên MongoDB:
   ```bash
   bash scripts/demo-data.sh nosql
   ```
   ✅ `insertedIds` có `demo-nosql-1/2/3`; `find` trả 2 tin có kỹ năng SQL; `matchedCount: 1, modifiedCount: 1`,
   `findOne` có `salary_max_vnd: 20000000` và `FastAPI` trong `skills`; `deletedCount: 1`; `aggregate` ra 2 nhóm
   (`01`, `79`); bảng Trino cuối có 2 dòng với `tinh_thanh` = *Thành phố Hà Nội*, *Thành phố Hồ Chí Minh*.

3. Một tin mới đi hết pipeline (~3–5 phút):
   ```bash
   bash scripts/demo-data.sh kafka "Kỹ sư dữ liệu (tin demo)" "15 - 25 triệu" "Đà Nẵng"
   ```
   ✅ Bước 1: in message JSON rồi `Đã gửi – xem trên Kafka UI`. Bước 2: `Job bronze_jobs đang chạy – đợi micro-batch
   kế tiếp`, rồi bảng 1 dòng (`synthetic | demo-…`). Bước 3: job silver chạy, kết thúc bằng `==> Xong: silver-job-postings-…`,
   rồi bảng 1 dòng: `salary_min_vnd` **15000000**, `salary_max_vnd` **25000000**, `location_codes` **[48]** (Đà Nẵng),
   `experience_min_years` 2.0, `level_code` **mid**.

## 6. Airflow (🟥 T4)

1. Dừng streaming, bật lại Airflow:
   ```bash
   bash scripts/demo-mode.sh airflow
   ```
   ✅ Dòng cuối `==> Đã bỏ tạm dừng DAG joblens_pipeline` và `Giờ mở Airflow → joblens_pipeline → Trigger`.
   T2 và T3 tự dừng (hoặc Ctrl+C) – **đóng T2, T3** được.
2. Trình duyệt Airflow (http://localhost:8082, user `admin`) → `joblens_pipeline` → **Trigger** → *Trigger* (để mặc định).
   ✅ Sau ~5–10 phút cả **5 ô task xanh**: `silver_job_postings` → `gx_validate_silver` → `dbt_build_gold`,
   `spark_analytics` → `iceberg_maintenance`. Bấm `gx_validate_silver` → *Logs*: `→ 15/15 kỳ vọng đạt`.
   Log không mở được (scheduler bận): đợi task xong, hoặc
   `kubectl logs -n compute $(kubectl get pods -n compute -o name | grep gx-silver | tail -1) | grep "kỳ vọng"`.
3. Grafana: bảng *Lần chạy spark_analytics* có dòng mới. Ghi lại ô *Số file dữ liệu* trước và sau DAG – đây là số đo
   còn thiếu cho ADR (`iceberg_maintenance` gộp file nhỏ của streaming; chưa đo).

## 7. Dọn dẹp và tắt máy

1. **T4** – sau khi DAG đã xong (tránh ghi cùng lúc với task silver):
   ```bash
   bash scripts/demo-data.sh clean
   ```
   ✅ `DROP TABLE`, `DROP SCHEMA`, `true`, `deletedCount`, rồi các dòng `DELETE: N rows` (N ≥ 1 cho tin `demo-…`).
2. **T1**: Ctrl+C để đóng `demo-ui.sh`. Rồi:
   ```bash
   k3d cluster stop joblens
   ```
   ✅ Không có dòng `ERRO`/`FATA`; `k3d cluster list` hiện `0/1` server. Dữ liệu được giữ.
3. (Tùy chọn) trả RAM cho Windows – **PowerShell** (không phải Ubuntu): `wsl --shutdown`, rồi thoát Docker Desktop.
4. Ghi một dòng vào `deploy/compat.md` với các số đo thật của lượt chạy (nếu là lượt chốt số liệu).

## 8. Máy mới (chưa có cụm)

Cần: Windows 10/11 + WSL2 Ubuntu, Docker Desktop (bật WSL integration), ≥ 16 GB RAM (8 GB: profile lite). Trong
Ubuntu cài `k3d`, `kubectl`, `helm`, `uv` theo [`../deploy/README.md`](../deploy/README.md). Rồi:

```bash
mkdir -p ~/joblens && cd ~/joblens
git clone https://github.com/BD-WM-SA-D/joblens.git
git clone https://github.com/BD-WM-SA-D/Big-Data-Storage-and-Processing-joblens.git
cd Big-Data-Storage-and-Processing-joblens
uv sync
bash scripts/bootstrap-all.sh                       # máy 8 GB: JOBLENS_PROFILE=lite bash scripts/bootstrap-all.sh
```

✅ ~30–40 phút sau, dòng cuối `########## Xong ##########` và gợi ý `bash scripts/demo-ui.sh`. Sau đó làm từ mục 2.
Lỗi giữa chừng: chạy lại đúng lệnh đó (mỗi bước bỏ qua phần đã có), rồi xem mục *Sự cố hay gặp* trong
[`demo.md`](demo.md).
