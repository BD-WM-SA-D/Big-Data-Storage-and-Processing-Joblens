-- gold.mart_skill_demand_monthly – contract joblens/schemas/tables/gold/mart_skill_demand_monthly.yaml 0.1.2
-- Nhu cầu kỹ năng theo tháng × nhóm nghề (UC002), từ silver.job_skills (WM) + silver.job_dedup (WM).
-- * Đếm theo NHÓM TIN TRÙNG (dup_group_id của silver.job_dedup; tin chưa qua dedup = chính nó): một tin đăng ở cả
--   TopCV và ITviec chỉ tính một lần. Nhóm lấy tháng sớm nhất và nhóm nghề của tin sớm nhất trong nhóm.
-- * Chỉ tin crawl: bỏ vietjobs (first_seen_ts = ngày nạp, dồn cả bộ vào một tháng) và synthetic (mô phỏng/demo).
-- * Tháng theo posted_date (ngày đăng trên nguồn), lùi về first_seen_ts khi null.
-- * share = số nhóm có kỹ năng / số nhóm trong tháng của nhóm nghề; mom_change = share − share tháng trước
--   (điểm %), tháng trước không có tin nào thì null; tháng trước có tin nhưng không có kỹ năng này thì tính share = 0.
with jobs as (
    select
        p.job_uid,
        coalesce(d.dup_group_id, p.job_uid) as grp,
        date_trunc('month', coalesce(p.posted_date, cast(p.first_seen_ts as date))) as month,
        coalesce(p.category_source, 'unknown') as category_source
    from {{ source('silver', 'job_postings') }} p
    left join {{ source('silver', 'job_dedup') }} d on d.job_uid = p.job_uid
    where p.source not in ('vietjobs', 'synthetic')
),

groups as (
    select grp, min(month) as month, min_by(category_source, month) as category_source
    from jobs
    group by grp
),

group_skills as (
    select distinct j.grp, s.skill_id
    from jobs j
    join {{ source('silver', 'job_skills') }} s on s.job_uid = j.job_uid
),

counts as (
    select g.month, g.category_source, gs.skill_id, count(*) as job_count
    from groups g join group_skills gs on gs.grp = g.grp
    group by g.month, g.category_source, gs.skill_id
    union all
    select g.month, 'ALL', gs.skill_id, count(*)
    from groups g join group_skills gs on gs.grp = g.grp
    group by g.month, gs.skill_id
),

totals as (
    select month, category_source, count(*) as total from groups group by month, category_source
    union all
    select month, 'ALL', count(*) from groups group by month
),

shares as (
    select c.month, c.category_source, c.skill_id, c.job_count, 1e0 * c.job_count / t.total as share
    from counts c
    join totals t on t.month = c.month and t.category_source = c.category_source
)

select
    s.month,
    s.skill_id,
    coalesce(k.label, s.skill_id) as skill_label,
    s.category_source,
    s.job_count,
    s.share,
    case
        when pt.total is null then null
        else 100e0 * (s.share - coalesce(ps.share, 0e0))
    end as mom_change
from shares s
left join {{ ref('dim_skill') }} k on k.skill_id = s.skill_id
left join totals pt
    on pt.category_source = s.category_source and pt.month = date_add('month', -1, s.month)
left join shares ps
    on ps.category_source = s.category_source and ps.skill_id = s.skill_id
   and ps.month = date_add('month', -1, s.month)
