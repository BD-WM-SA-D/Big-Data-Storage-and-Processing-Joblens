{#- Test tự viết để khỏi cần package dbt_utils (không tải package lúc chạy). -#}

{% test row_count_equals(model, expected) %}
select count(*) as n from {{ model }} having count(*) <> {{ expected }}
{% endtest %}

{% test relationships_or_all(model, column_name, to, field) %}
-- mọi mã phải có trong bảng đích, trừ dòng tổng 'ALL' và 'unknown'
select m.{{ column_name }}
from {{ model }} m
left join {{ to }} d on m.{{ column_name }} = d.{{ field }}
where m.{{ column_name }} not in ('ALL', 'unknown') and d.{{ field }} is null
{% endtest %}

{% test unique_combination(model, columns) %}
-- khóa nhiều cột không trùng (thay dbt_utils.unique_combination_of_columns)
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test one_current_per_key(model, key) %}
-- SCD loại 2: mỗi khóa đúng một phiên bản hiện tại (is_current), và phiên bản đó là phiên bản lớn nhất
select {{ key }}
from {{ model }}
group by {{ key }}
having count_if(is_current) <> 1
    or max(version_no) <> max(case when is_current then version_no end)
{% endtest %}

{% test valid_intervals(model, key) %}
-- SCD loại 2: khoảng hiệu lực liền nhau, không chồng: valid_to của phiên bản n = valid_from của phiên bản n + 1,
-- valid_from < valid_to, version_no liên tục từ 1
select *
from (
    select
        {{ key }}, version_no, valid_from, valid_to,
        lead(valid_from) over (partition by {{ key }} order by version_no) as next_from,
        row_number() over (partition by {{ key }} order by version_no) as rn
    from {{ model }}
) v
where version_no <> rn
   or (valid_to is not null and valid_to <= valid_from)
   or (next_from is not null and valid_to is distinct from next_from)
   or (next_from is null and valid_to is not null)
{% endtest %}
