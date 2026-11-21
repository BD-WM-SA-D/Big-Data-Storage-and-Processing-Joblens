-- gold.dim_skill – contract joblens/schemas/tables/gold/dim_skill.yaml 0.2.0
-- Danh mục kỹ năng xuất hiện trong silver.job_skills (image skill-extractor của Web Mining ghi).
-- label = cách viết gặp nhiều nhất của kỹ năng đó; aliases = mọi cách viết đã gặp (sắp xếp).
-- esco_uri, skill_type để null: WM chưa gắn ESCO (models.yaml của WM: cột esco_uri trong từ điển còn rỗng).
-- Khi WM giao danh mục kỹ năng chính thức (nhãn + ESCO) thì đổi label/esco_uri sang đọc danh mục đó.
with forms as (
    select skill_id, surface_form, count(*) as n
    from {{ source('silver', 'job_skills') }}
    group by skill_id, surface_form
),

ranked as (
    select
        skill_id,
        surface_form,
        row_number() over (partition by skill_id order by n desc, surface_form) as rn
    from forms
)

select
    skill_id,
    max(case when rn = 1 then surface_form end) as label,
    cast(null as varchar) as esco_uri,
    cast(null as varchar) as skill_type,
    array_sort(array_agg(surface_form)) as aliases
from ranked
group by skill_id
