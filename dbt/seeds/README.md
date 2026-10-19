# dbt seeds

## `locations_2025.csv` → `gold.dim_location` (qua model dbt `dim_location`)

34 đơn vị hành chính cấp tỉnh sau sắp xếp 2025.

- **Mã số và tên:** theo Quyết định 19/2025/QĐ-TTg (Bảng danh mục và mã số các đơn vị hành chính Việt Nam, hiệu lực 01/07/2025), bảng trích tại [thuvienphapluat.vn](https://thuvienphapluat.vn/banan/tin-tuc/chinh-thuc-bang-danh-muc-ma-so-cua-34-tinh-thanh-moi-va-cac-don-vi-hanh-chinh-cap-xa-sau-sap-nhap-17030.html). Lưu ý mã của đơn vị sau sáp nhập không phải lúc nào cũng là mã của tỉnh giữ tên (ví dụ Bắc Ninh 24, Quảng Trị 44, Đồng Tháp 82).
- **Tỉnh cũ gộp vào tỉnh mới:** theo Nghị quyết 202/2025/QH15 về sắp xếp đơn vị hành chính cấp tỉnh ([Báo Lào Cai](https://baolaocai.vn/nghi-quyet-cua-quoc-hoi-ve-sap-xep-don-vi-hanh-chinh-cap-tinh-post403219.html)). Tên tỉnh cũ nằm trong cột `aliases` để tin ghi địa điểm cũ (ví dụ "Bình Dương", "TP HCM (cũ)") vẫn ánh xạ đúng.
- **`region`:** north | central | south theo cách chia ba miền thông dụng (Tây Nguyên xếp vào central).
- **`aliases`:** các cách viết, nối bằng `|` (CSV không chứa được danh sách; model dbt tách thành mảng). So khớp không phân biệt hoa thường, dấu, tiền tố "Tỉnh/Thành phố/TP" và hậu tố "(cũ)" – xem `spark-jobs/normalize.py`. Thêm alias khi thấy địa điểm chưa ánh xạ được trong log của job silver.

File này là **nguồn duy nhất** cho ánh xạ địa điểm: job Spark silver đọc chính file này (đóng vào image), dbt seed nạp thành bảng gold.
- **Quận/huyện, thành phố trực thuộc tỉnh:** thêm vào `aliases` các tên hay gặp trong tin tuyển dụng (quận/huyện cũ của Hà Nội và TP.HCM, thành phố thuộc Bình Dương và Bà Rịa - Vũng Tàu cũ, Vinh, Buôn Ma Thuột, Long Khánh) – lấy từ `scripts/check-locations.sql` trên 48.092 tin VietJobs. Tên trùng giữa nhiều tỉnh (ví dụ "Tân Phú" cũng là huyện của Đồng Nai) gán cho nơi xuất hiện nhiều hơn trong dữ liệu.
