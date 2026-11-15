-- gold.mart_job_changes_weekly – contract joblens/schemas/tables/gold/mart_job_changes_weekly.yaml 0.1.0
-- Tin đổi gì theo tuần × nguồn × nhóm trường (UC003), đọc silver.job_postings_history (SCD loại 2).
-- Mỗi phiên bản > 1 là một lần nhà tuyển dụng sửa tin; changed_fields cho biết sửa cột nào, so với phiên bản
-- trước (lag theo version_no) để biết lương tăng hay giảm bao nhiêu %.
-- Tăng dần như mart_hiring_weekly: tuần có phiên bản mới/sửa (updated_ts > updated_through) thì tính lại cả tuần.
-- Hàm lag cần phiên bản trước – có thể nằm ở tuần cũ – nên đọc toàn bộ lịch sử CỦA CÁC TIN có thay đổi trong
-- các tuần đó (lọc theo job_uid), không đọc cả bảng.
-- Giới hạn: bản crawl đến trễ có thể dời valid_from của một phiên bản sang tuần khác; tuần cũ còn số cũ tới lần
-- --full-refresh.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='week',
    on_schema_change='fail',
    properties={"partitioning": "ARRAY['month(week)']"}
) }}

with hist as (
    select
        job_uid,
        source,
        version_no,
        valid_from,
        changed_fields,
        updated_ts,
        cast(date_trunc('week', valid_from) as date) as week,
        case
            when salary_min_vnd is not null and salary_max_vnd is not null then (salary_min_vnd + salary_max_vnd) / 2
            else coalesce(salary_min_vnd, salary_max_vnd)
        end as salary_mid_vnd
    from {{ source('silver', 'job_postings_history') }}
),

{% if is_incremental() %}
affected as (
    select distinct week
    from hist
    where version_no > 1
      and updated_ts > (select coalesce(max(updated_through), timestamp '1970-01-01 00:00:00 UTC') from {{ this }})
),
{% endif %}

scoped as (
    select * from hist
    {% if is_incremental() %}
    where job_uid in (select job_uid from hist where version_no > 1 and week in (select week from affected))
    {% endif %}
),

versions as (
    select
        *,
        lag(salary_mid_vnd) over (partition by job_uid order by version_no) as prev_salary_mid_vnd,
        min(valid_from) over (partition by job_uid) as first_valid_from
    from scoped
),

changes as (
    select
        v.week,
        v.source,
        v.job_uid,
        f as changed_field,
        v.salary_mid_vnd,
        v.prev_salary_mid_vnd,
        date_diff('second', v.first_valid_from, v.valid_from) / 86400e0 as days_since_first_seen
    from versions v
    cross join unnest(array_distinct(transform(v.changed_fields, x ->
        case
            when x like 'salary%' then 'salary'
            when x like 'experience%' then 'experience'
            when x = 'company_key' then 'company'
            when x = 'location_codes' then 'location'
            when x = 'level_code' then 'level'
            when x = 'employment_type_code' then 'employment_type'
            else x
        end))) as t (f)
    where v.version_no > 1
    {% if is_incremental() %} and v.week in (select week from affected) {% endif %}
)

select
    week,
    source,
    changed_field,
    count(*) as change_count,
    count(distinct job_uid) as job_count,
    case when changed_field = 'salary' then count_if(salary_mid_vnd > prev_salary_mid_vnd) end as salary_raise_count,
    case when changed_field = 'salary' then count_if(salary_mid_vnd < prev_salary_mid_vnd) end as salary_cut_count,
    case when changed_field = 'salary' then approx_percentile(
        case when prev_salary_mid_vnd > 0 then 100e0 * (salary_mid_vnd - prev_salary_mid_vnd) / prev_salary_mid_vnd end,
        0.5) end as salary_change_median_pct,
    approx_percentile(days_since_first_seen, 0.5) as days_since_first_seen_median,
    (select max(updated_ts) from hist) as updated_through
from changes
group by week, source, changed_field
