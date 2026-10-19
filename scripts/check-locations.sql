-- Địa điểm gốc chưa ánh xạ được về 34 tỉnh/thành (silver.location_codes rỗng), nhiều nhất trước.
-- Dùng để bổ sung alias vào dbt/seeds/locations_2025.csv. Chạy: bash scripts/trino.sh -f scripts/check-locations.sql
SELECT json_format(json_extract(b.payload_json, '$.locations')) AS dia_diem_goc, count(*) AS so_tin
FROM iceberg.bronze.jobs_raw b
JOIN iceberg.silver.job_postings s ON s.source = b.source AND s.source_job_id = b.source_job_id
WHERE cardinality(s.location_codes) = 0
GROUP BY 1 ORDER BY 2 DESC LIMIT 30;
