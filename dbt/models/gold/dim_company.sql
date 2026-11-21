-- gold.dim_company – contract joblens/schemas/tables/gold/dim_company.yaml 0.1.0
-- Công ty gộp giữa tin tuyển dụng (silver.job_postings) và review (silver.review_aspects của WM) theo company_key.
-- * company_key của tin: normalize.company_key (repo này); của review: absa-scorer (WM) tính theo cùng quy tắc nhưng là
--   bản code riêng – đề xuất đưa vào joblens_contracts (issue #2); hai bản lệch thì một công ty tách thành hai dòng.
-- * job_count đếm theo nhóm tin trùng (silver.job_dedup); review_count = số review có ít nhất một khía cạnh.
-- * Bỏ synthetic (mô phỏng/demo). company_name = tên gặp nhiều nhất (tin trước, review sau), thiếu thì company_key.
with jobs as (
    select p.company_key, p.company_name, p.source, coalesce(d.dup_group_id, p.job_uid) as grp
    from {{ source('silver', 'job_postings') }} p
    left join {{ source('silver', 'job_dedup') }} d on d.job_uid = p.job_uid
    where p.company_key is not null and p.source <> 'synthetic'
),

reviews as (
    select distinct
        a.company_key,
        a.review_uid,
        b.source,
        json_extract_scalar(b.payload_json, '$.company_name') as company_name
    from {{ source('silver', 'review_aspects') }} a
    join {{ source('bronze', 'reviews_raw') }} b
        on lower(to_hex(sha1(to_utf8(b.source || ':' || b.source_review_id)))) = a.review_uid
    where b.source <> 'synthetic'
),

names as (
    select company_key, company_name, n, prio,
        row_number() over (partition by company_key order by prio, n desc, company_name) as rn
    from (
        select company_key, company_name, count(*) as n, 1 as prio from jobs
        where company_name is not null group by company_key, company_name
        union all
        select company_key, company_name, count(distinct review_uid), 2 from reviews
        where company_name is not null group by company_key, company_name
    ) x
),

job_side as (
    select company_key, count(distinct grp) as job_count, array_agg(distinct source) as sources
    from jobs group by company_key
),

review_side as (
    select company_key, count(distinct review_uid) as review_count, array_agg(distinct source) as sources
    from reviews group by company_key
)

select
    coalesce(j.company_key, r.company_key) as company_key,
    coalesce(n.company_name, coalesce(j.company_key, r.company_key)) as company_name,
    array_sort(array_distinct(concat(
        coalesce(j.sources, cast(array[] as array(varchar))),
        coalesce(r.sources, cast(array[] as array(varchar)))
    ))) as sources,
    coalesce(j.job_count, 0) as job_count,
    coalesce(r.review_count, 0) as review_count
from job_side j
full outer join review_side r on r.company_key = j.company_key
left join names n on n.company_key = coalesce(j.company_key, r.company_key) and n.rn = 1
