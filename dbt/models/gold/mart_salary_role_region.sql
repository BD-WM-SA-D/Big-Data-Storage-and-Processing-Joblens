-- gold.mart_salary_role_region – contract joblens/schemas/tables/gold/mart_salary_role_region.yaml 0.1.1
-- Mặt bằng lương theo tháng × nhóm nghề × cấp bậc × tỉnh/thành, có dòng tổng 'ALL' cho từng chiều (UC003).
-- Ghi chú:
-- * job_count đếm theo job_uid: silver.job_dedup (WM) chưa có – khi có thì đổi sang dup_group_id (ADR 0001 D5).
-- * Tin nhiều địa điểm được tách thành nhiều dòng; dòng 'ALL' đếm distinct nên không đếm trùng.
-- * Trung điểm lương: (min+max)/2; chỉ có một cận thì dùng cận đó. Phân vị null khi có lương < 5 tin.
-- * Bỏ tin synthetic (fixtures). Tin VietJobs giữ lại – tháng của chúng là tháng nạp (docs/13).
{{ config(properties={"partitioning": "ARRAY['month']"}) }}

with jobs as (
    select
        job_uid,
        cast(date_trunc('month', coalesce(posted_date, cast(first_seen_ts as date))) as date) as month,
        coalesce(category_source, 'unknown') as category_source,
        coalesce(level_code, 'unknown') as level_code,
        case when cardinality(location_codes) = 0 then array['unknown'] else location_codes end as locs,
        salary_is_negotiable,
        case
            when salary_min_vnd is not null and salary_max_vnd is not null then (salary_min_vnd + salary_max_vnd) / 2
            else coalesce(salary_min_vnd, salary_max_vnd)
        end as salary_mid_vnd
    from {{ source('silver', 'job_postings') }}
    where source <> 'synthetic'
),

exploded as (
    select j.*, loc as location_code
    from jobs j
    cross join unnest(j.locs) as t (loc)
),

agg as (
    select
        month,
        coalesce(category_source, 'ALL') as category_source,
        coalesce(level_code, 'ALL') as level_code,
        coalesce(location_code, 'ALL') as location_code,
        count(distinct job_uid) as job_count,
        count(distinct case when salary_mid_vnd is not null then job_uid end) as salary_job_count,
        approx_percentile(salary_mid_vnd, 0.25) as p25,
        approx_percentile(salary_mid_vnd, 0.50) as p50,
        approx_percentile(salary_mid_vnd, 0.75) as p75,
        avg(case when salary_is_negotiable then 1e0 else 0e0 end) as negotiable_share  -- double (1.0 là DECIMAL(2,1) trong Trino)
    from exploded
    group by grouping sets (
        (month, category_source, level_code, location_code),
        (month, category_source, level_code),
        (month, category_source, location_code),
        (month, level_code, location_code),
        (month, category_source),
        (month, level_code),
        (month, location_code),
        (month)
    )
)

select
    month,
    category_source,
    level_code,
    location_code,
    job_count,
    case when salary_job_count >= 5 then cast(p25 as bigint) end as salary_p25_vnd,
    case when salary_job_count >= 5 then cast(p50 as bigint) end as salary_median_vnd,
    case when salary_job_count >= 5 then cast(p75 as bigint) end as salary_p75_vnd,
    negotiable_share
from agg
