-- gold.mart_hiring_weekly – contract joblens/schemas/tables/gold/mart_hiring_weekly.yaml 0.1.0
-- Tin mới theo tuần × nguồn × tỉnh/thành × cấp bậc (UC002). Mô hình TĂNG DẦN (incremental):
-- * Lần đầu (hoặc --full-refresh): dựng mọi tuần.
-- * Các lần sau: chỉ tính lại những TUẦN có ít nhất một tin đổi từ lần trước (updated_ts > updated_through lớn nhất
--   đang có), rồi delete+insert đúng các tuần đó. Tuần không đổi giữ nguyên – không quét lại, không ghi lại.
--   Tính lại cả tuần (thay vì cộng dồn từng tin) vì trung vị lương và count distinct không cộng dồn được.
-- * Giới hạn: tuần mà mọi tin của nó bị xóa khỏi silver thì không có dòng nào báo "đổi" → còn số cũ tới lần
--   --full-refresh (xóa tin hiếm, chỉ có ở scripts/demo-data.sh clean, recrawl_sim --clean).
-- Có cả tin vietjobs (tuần = tuần nạp) và synthetic (mô phỏng): lọc theo cột source khi báo cáo thị trường.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='week',
    on_schema_change='fail',
    properties={"partitioning": "ARRAY['month(week)']"}
) }}

with src as (
    select
        job_uid,
        source,
        cast(date_trunc('week', first_seen_ts) as date) as week,
        coalesce(level_code, 'unknown') as level_code,
        case when cardinality(location_codes) = 0 then array['unknown'] else location_codes end as locs,
        salary_is_negotiable,
        case
            when salary_min_vnd is not null and salary_max_vnd is not null then (salary_min_vnd + salary_max_vnd) / 2
            else coalesce(salary_min_vnd, salary_max_vnd)
        end as salary_mid_vnd,
        updated_ts
    from {{ source('silver', 'job_postings') }}
),

{% if is_incremental() %}
affected as (
    select distinct week
    from src
    where updated_ts > (select coalesce(max(updated_through), timestamp '1970-01-01 00:00:00 UTC') from {{ this }})
),
{% endif %}

jobs as (
    select * from src
    {% if is_incremental() %} where week in (select week from affected) {% endif %}
),

exploded as (
    select j.*, loc as location_code
    from jobs j
    cross join unnest(j.locs) as t (loc)
),

-- Tin nhiều địa điểm có ở nhiều tỉnh, nhưng dòng 'ALL' tính trên jobs (mỗi tin một dòng) – không dùng
-- grouping sets trên exploded, vì như vậy trung vị và tỉ lệ thỏa thuận sẽ đếm tin nhiều địa điểm nhiều lần.
agg as (
    select
        week, source, location_code, level_code,
        count(distinct job_uid) as new_job_count,
        count(distinct case when salary_mid_vnd is not null then job_uid end) as salary_job_count,
        approx_percentile(salary_mid_vnd, 0.5) as p50,
        avg(case when salary_is_negotiable then 1e0 else 0e0 end) as negotiable_share
    from exploded
    group by week, source, location_code, level_code
    union all
    select
        week, source, 'ALL' as location_code, level_code,
        count(*) as new_job_count,
        count(salary_mid_vnd) as salary_job_count,
        approx_percentile(salary_mid_vnd, 0.5) as p50,
        avg(case when salary_is_negotiable then 1e0 else 0e0 end) as negotiable_share
    from jobs
    group by week, source, level_code
)

select
    week,
    source,
    location_code,
    level_code,
    new_job_count,
    salary_job_count,
    case when salary_job_count >= 5 then cast(p50 as bigint) end as salary_median_vnd,
    negotiable_share,
    (select max(updated_ts) from src) as updated_through
from agg
