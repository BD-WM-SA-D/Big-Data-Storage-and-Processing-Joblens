-- gold.mart_company_aspect_sentiment – contract joblens/schemas/tables/gold/mart_company_aspect_sentiment.yaml 0.1.1
-- Đánh giá công ty theo khía cạnh (UC005): tỉ lệ câu tích cực/tiêu cực/trung tính từ silver.review_aspects (image
-- absa-scorer của WM) cạnh điểm sao ITviec của cùng khía cạnh (payload.aspect_ratings trong bronze.reviews_raw).
-- * avg_star_rating: trung bình sao của khía cạnh trên các review của công ty (bản crawl mới nhất của mỗi review);
--   work_life_balance không có sao trên ITviec → null.
-- * Bỏ review synthetic (fixture/demo). model_version: bản mô hình mới nhất đã chấm công ty × khía cạnh đó.
with raw as (
    select
        lower(to_hex(sha1(to_utf8(source || ':' || source_review_id)))) as review_uid,
        payload_json,
        row_number() over (partition by source, source_review_id order by crawl_ts desc, kafka_offset desc) as rn
    from {{ source('bronze', 'reviews_raw') }}
    where source <> 'synthetic'
),

latest as (select review_uid, payload_json from raw where rn = 1),

aspects as (
    select a.company_key, a.review_uid, a.aspect, a.polarity, a.model_version
    from {{ source('silver', 'review_aspects') }} a
    join latest l on l.review_uid = a.review_uid
),

company_reviews as (
    select distinct a.company_key, a.review_uid, l.payload_json
    from aspects a join latest l on l.review_uid = a.review_uid
),

stars as (
    select company_key, aspect, avg(star) as avg_star_rating
    from (
        select
            c.company_key,
            t.aspect,
            try_cast(json_extract_scalar(c.payload_json, t.path) as double) as star
        from company_reviews c
        cross join (values
            ('salary_benefits', '$.aspect_ratings.salary_benefits'),
            ('training_learning', '$.aspect_ratings.training_learning'),
            ('management_care', '$.aspect_ratings.management_care'),
            ('culture_fun', '$.aspect_ratings.culture_fun'),
            ('office_workspace', '$.aspect_ratings.office_workspace')
        ) as t (aspect, path)
    ) x
    group by company_key, aspect
),

agg as (
    select
        company_key,
        aspect,
        count(*) as sentence_count,
        avg(case when polarity = 'positive' then 1e0 else 0e0 end) as positive_share,
        avg(case when polarity = 'negative' then 1e0 else 0e0 end) as negative_share,
        avg(case when polarity = 'neutral' then 1e0 else 0e0 end) as neutral_share,
        max(model_version) as model_version
    from aspects
    group by company_key, aspect
)

select
    g.company_key,
    coalesce(c.company_name, g.company_key) as company_name,
    g.aspect,
    g.sentence_count,
    g.positive_share,
    g.negative_share,
    g.neutral_share,
    s.avg_star_rating,
    g.model_version
from agg g
left join stars s on s.company_key = g.company_key and s.aspect = g.aspect
left join {{ ref('dim_company') }} c on c.company_key = g.company_key
