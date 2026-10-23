# Job phân tích Spark – kết quả

Số liệu do `spark-jobs/analytics_spark.py` (task `spark_analytics` của DAG `joblens_pipeline`) ghi vào MongoDB
`joblens.analytics_runs`; đọc bằng
`bash scripts/trino.sh "SELECT … FROM mongodb.joblens.analytics_runs ORDER BY run_at DESC LIMIT 1"`.
Ghi nguyên văn – không làm tròn lại khi dùng trong báo cáo ngoài mức làm tròn ghi ở đây.

## Lần 1 – 2026-10-23 21:41–21:46 (giờ VN)

- Commit `157f045`, profile `full`, Spark cluster mode 1 executor; DAG chạy thủ công, 5/5 task thành công.
- Dữ liệu: silver.job_postings 48.092 tin (bỏ 3 tin synthetic).

**Kế hoạch thực thi (cờ `plan_checks`):** broadcast_join = true (BroadcastHashJoin), sort_merge_join = true
(SortMergeJoin), pivot_has_levels = true, bronze_filter_pushed = true (bộ lọc `ingest_ts` đẩy xuống scan Iceberg).
Kế hoạch đầy đủ: `s3://joblens/reports/spark-plans/<thời điểm>/`. Pivot và unpivot cho cùng tổng (true).

**Thống kê:** tương quan kinh nghiệm tối thiểu – lương trung điểm 0,280 (34.363 tin có cả hai). Phân vị lương trung
điểm: p10 8,15 · p25 10,5 · p50 13,0 · p75 17,5 · p90 25,0 triệu VND/tháng.

**MLlib** (LinearRegression trên log(lương), đặc trưng: cấp bậc, vùng, kinh nghiệm; CrossValidator 3 fold × 4 tổ hợp):

| | Giá trị |
|---|---|
| Train / test | 27.189 / 6.788 tin |
| Tham số tốt nhất | regParam 0,0 · elasticNet 0,0 |
| RMSE log (CV) | 0,412 |
| R² log (test) | 0,370 |
| RMSE test | 10,32 triệu VND |
| RMSE mô hình "luôn đoán trung vị" | 11,20 triệu VND |
| Thời gian fit (CV) | 176,8 giây |

Nhận xét: mô hình chỉ tốt hơn mô hình cơ sở ~8% về RMSE (10,32 so với 11,20) – cấp bậc, vùng và kinh nghiệm giải thích
được một phần nhỏ biến thiên lương; thiếu đặc trưng nghề/kỹ năng (kỹ năng đến từ repo Web Mining – `silver.job_skills`).
Đây là kết quả trung thực để trình bày, không phải mô hình dự đoán lương dùng được.

**Lương theo tỉnh (5 tỉnh nhiều tin nhất):**

| Tỉnh | Tin | Trung vị (triệu) | TB cắt 10% (triệu) | Hạng trong vùng |
|---|---|---|---|---|
| Thành phố Hà Nội | 21.929 | 13,5 | 14,52 | 6 |
| Thành phố Hồ Chí Minh | 18.332 | 12,5 | 13,70 | 3 |
| Thành phố Hải Phòng | 1.126 | 13,5 | 14,47 | 6 |
| Bắc Ninh | 1.099 | 14,5 | 15,14 | 4 |
| Thành phố Đà Nẵng | 1.086 | 11,5 | 12,37 | 5 |

Trung vị ở đây là `percentile_approx` của Spark trên lương trung điểm, nên có thể lệch nhẹ so với
`approx_percentile` của Trino ở `gold.mart_salary_role_region` (13,75 cho Hà Nội) – hai thuật toán xấp xỉ khác nhau.
