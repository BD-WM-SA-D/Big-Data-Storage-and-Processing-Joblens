# Đóng góp vào JobLens

Bản rút gọn của [Quy tắc làm việc chung](https://github.com/BD-WM-SA-D/joblens/blob/main/docs/project-plan/09-quy-tac-lam-viec-chung.md) – áp dụng như nhau ở cả 4 repo.

## Luồng làm việc

1. Nhận hoặc tạo issue trên board chung của org; mỗi issue một người phụ trách.
2. Tạo nhánh `feat/<mô-tả-ngắn>`, `fix/…` hoặc `docs/…` từ `main`.
3. Commit theo Conventional Commits: `feat(crawler): …`, `fix(dbt): …`, `docs(uml): …`.
4. Mở PR và **tự review**: đọc lại toàn bộ diff theo checklist bên dưới.
5. Squash merge khi CI pass. Không push thẳng `main`.
6. Ngoại lệ: nếu PR có thể làm hỏng repo khác (contracts, đổi tên cột/bảng/topic), báo trong nhóm chat kèm link và chờ 24 giờ; thay đổi *major* chờ người phụ trách repo bị ảnh hưởng xác nhận.

## Khi đụng tới schema

Mở PR ở `joblens-contracts` trước (SemVer: thêm trường không bắt buộc → minor; đổi tên/xóa → major kèm topic/bảng `.vN` mới). Chỉ sửa code ở repo này sau khi contracts đã được merge và gắn tag.

## Không được commit

Danh sách đầy đủ: [quy tắc 8](https://github.com/BD-WM-SA-D/joblens/blob/main/docs/project-plan/09-quy-tac-lam-viec-chung.md#8-những-gì-không-được-đưa-lên-github). Tóm tắt:

- Bí mật (`.env`, key, token, kubeconfig) → `.env` đã gitignore hoặc Kubernetes Secret.
- Dữ liệu crawl, tập nhãn, model, output notebook → object storage `s3://joblens/…`.
- Dữ liệu cá nhân → không lưu ở đâu cả.
- Slide/tài liệu có bản quyền của giảng viên.

Sau khi clone, chạy `pre-commit install` để các lệnh chặn tự động hoạt động. Lỡ đẩy bí mật lên: đổi key ngay rồi báo nhóm.

## Môi trường

- Python 3.12; `ruff`, `pytest`, `pre-commit` (chạy `pre-commit install` sau khi clone).
- Biến môi trường theo tiền tố `JOBLENS_` – xem `.env.example`.

## Checklist PR (tự review)

- [ ] Đã tự đọc lại toàn bộ diff trên GitHub.
- [ ] PR nhỏ, một mục đích.
- [ ] Test/lint pass; không có dữ liệu hay bí mật trong diff.
- [ ] Cập nhật tài liệu/ADR nếu thay đổi hành vi hoặc kiến trúc.
- [ ] Ghi chú nếu có dùng AI (công cụ, phần nào, ai đã kiểm tra).
- [ ] Nếu ảnh hưởng repo khác: gắn `cross-repo`, đã báo nhóm và chờ đủ 24 giờ.
