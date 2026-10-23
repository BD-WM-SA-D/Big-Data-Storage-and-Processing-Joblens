# Chạy demo trên cloud miễn phí – GitHub Codespaces

Môn học yêu cầu triển khai trên **Kubernetes hoặc Cloud** (chỉ Docker thì không khuyến khích). Nền tảng chạy trên
Kubernetes (k3d) ở cả máy cá nhân lẫn Codespaces. Codespaces cho thêm một máy cloud không cần thẻ tín dụng, đủ RAM
cho toàn bộ hệ thống – dùng cho buổi demo khi máy cá nhân yếu, hoặc làm phương án dự phòng.

> Hạn mức và giá dưới đây lấy từ tài liệu GitHub (tháng 10/2026) – kiểm tra lại trước khi dùng:
> <https://docs.github.com/billing/managing-billing-for-github-codespaces/about-billing-for-github-codespaces>

## Hạn mức miễn phí (tài khoản cá nhân)

| Tài khoản | Giờ máy / tháng | Lưu trữ / tháng |
|---|---|---|
| GitHub Free | 120 core-giờ | 15 GB-tháng |
| GitHub Pro (có trong GitHub Student Developer Pack) | 180 core-giờ | 20 GB-tháng |

Máy 4 nhân / 16 GB tính **4 core-giờ cho mỗi giờ chạy** → Free: ~30 giờ/tháng, Pro: ~45 giờ/tháng. Vượt hạn mức thì
bị tính tiền vào tài khoản cá nhân – đặt **Spending limit = 0 USD** ở <https://github.com/settings/billing> để GitHub
dừng codespace thay vì tính tiền. Codespace đã *dừng* chỉ tốn lưu trữ; *xóa* codespace khi không cần nữa.

## Tạo codespace (một lần, ~10 phút)

1. Mở repo `BD-WM-SA-D/Big-Data-Storage-and-Processing-Joblens` trên GitHub → **Code** → tab **Codespaces** →
   **⋯** → **New with options…**
2. Branch `main`, **Machine type: 4-core, 16 GB RAM**, Region gần nhất (Southeast Asia). **Create codespace**.
3. Đợi `.devcontainer/post-create.sh` cài kubectl, helm, k3d, uv và chạy `uv sync` (cửa sổ terminal báo "Xong").
4. Settings của codespace (<https://github.com/settings/codespaces>): **Default idle timeout** = 120 phút cho ngày
   demo (mặc định 30 phút – codespace tự dừng khi không gõ phím).

Repo của tổ chức chưa bật thanh toán Codespaces thì codespace tính vào hạn mức cá nhân của người tạo. Nếu không tạo
được, fork repo về tài khoản cá nhân rồi tạo codespace từ bản fork.

## Dựng nền tảng (~30–40 phút lần đầu)

Trong terminal của codespace:

```bash
bash scripts/bootstrap-all.sh
```

Script tạo cụm k3d một node (`K3D_AGENTS=0` – tiết kiệm đĩa, mỗi node giữ một bản image), cài Kafka, SeaweedFS +
Lakekeeper, MongoDB, Trino, tải VietJobs từ Hugging Face, chạy batch → silver → dbt, rồi cài Grafana và Airflow.

Mở giao diện: `bash scripts/demo-ui.sh` → tab **PORTS** của VS Code (trình duyệt) → bấm biểu tượng quả cầu ở cổng
3000 (Grafana), 8082 (Airflow), 8081 (Kafka UI)… Cổng để chế độ **Private** (chỉ tài khoản của bạn mở được) – trình
chiếu từ trình duyệt của mình, không cần chia sẻ link.

## Ngày demo

- Hôm trước: dựng xong, chạy thử `docs/demo.md` mục 3, rồi **Stop codespace** (giữ nguyên cụm và dữ liệu, không tốn
  giờ máy).
- Trước giờ demo 30 phút: mở lại codespace, rồi
  ```bash
  k3d cluster stop joblens && k3d cluster start joblens   # Docker khởi động lại → node đổi IP, phải start lại cụm
  kubectl get pods -A                                     # đợi Running (3–5 phút)
  bash scripts/demo-ui.sh
  ```
- Sau môn học: xóa codespace (<https://github.com/codespaces>) để không tốn lưu trữ.

## Giới hạn cần biết

- Mạng trường chặn `*.github.dev` hoặc websocket thì không mở được cổng – mang theo phương án dự phòng (video, máy cá nhân).
- Đĩa 32 GB: đủ cho image + VietJobs + bảng Iceberg. `docker system df` để xem; `docker image prune` xóa image cũ
  sau nhiều lần build.
- Codespaces là máy dev, không phải hạ tầng sản xuất: không mở cổng công khai, không đưa dữ liệu cá nhân lên.
- Phương án khác đã cân nhắc (10/2026): Azure for Students (100 USD, không cần thẻ, VM giữ được lâu hơn nhưng phải tự
  cài), Oracle Always Free (từ 06/2026 chỉ còn 2 nhân ARM / 12 GB, cần thẻ).
