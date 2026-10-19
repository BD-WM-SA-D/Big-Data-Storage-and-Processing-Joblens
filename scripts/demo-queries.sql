-- Truy vấn cho buổi demo Big Data (docs/demo.md). Chạy: bash scripts/trino.sh -f scripts/demo-queries.sql

-- 1. Bronze: dữ liệu thô theo nguồn và đường nạp (backfill.vietjobs = batch, jobs.raw.* = streaming)
SELECT source, kafka_topic, count(*) AS so_dong, count(DISTINCT source_job_id) AS so_tin, max(ingest_ts) AS lan_nap_cuoi
FROM iceberg.bronze.jobs_raw GROUP BY 1, 2 ORDER BY 1, 2;

-- 2. Silver: một dòng một tin dù bronze có bản trùng (MERGE INTO theo job_uid)
SELECT source, count(*) AS so_tin, sum(CAST(is_active AS INT)) AS dang_mo,
       round(avg(CAST(cardinality(location_codes) > 0 AS INT)), 3) AS ti_le_co_dia_diem,
       round(avg(CAST(salary_min_vnd IS NOT NULL OR salary_max_vnd IS NOT NULL AS INT)), 3) AS ti_le_co_luong
FROM iceberg.silver.job_postings GROUP BY 1;

-- 3. Chuẩn hóa: chuỗi gốc → giá trị chuẩn
SELECT salary_text, salary_min_vnd, salary_max_vnd, salary_is_negotiable, location_codes, experience_min_years, level_code
FROM iceberg.silver.job_postings WHERE source = 'vietjobs' LIMIT 10;

-- 4. Gold: lương trung vị theo tỉnh (toàn bộ nhóm nghề, mọi cấp bậc)
SELECT d.name AS tinh_thanh, m.job_count AS so_tin, m.salary_median_vnd / 1000000.0 AS trung_vi_trieu,
       m.salary_p25_vnd / 1000000.0 AS p25_trieu, m.salary_p75_vnd / 1000000.0 AS p75_trieu
FROM iceberg.gold.mart_salary_role_region m JOIN iceberg.gold.dim_location d USING (location_code)
WHERE m.category_source = 'ALL' AND m.level_code = 'ALL'
ORDER BY m.job_count DESC LIMIT 10;

-- 5. Gold: lương theo cấp bậc, toàn quốc
SELECT level_code, job_count, salary_median_vnd / 1000000.0 AS trung_vi_trieu, round(negotiable_share, 3) AS ti_le_thoa_thuan
FROM iceberg.gold.mart_salary_role_region
WHERE category_source = 'ALL' AND location_code = 'ALL' AND level_code <> 'ALL'
ORDER BY salary_median_vnd DESC NULLS LAST;

-- 6. Iceberg: lịch sử snapshot của silver (mỗi lần MERGE là một snapshot) – time travel được
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS them, summary['deleted-records'] AS xoa
FROM iceberg.silver."job_postings$snapshots" ORDER BY committed_at;

-- 7. Iceberg: số file và kích thước trung bình (sức khỏe bảng – nhiều file nhỏ do streaming)
SELECT count(*) AS so_file, round(avg(file_size_in_bytes) / 1024.0, 1) AS kb_trung_binh, sum(record_count) AS so_dong
FROM iceberg.bronze."jobs_raw$files";
