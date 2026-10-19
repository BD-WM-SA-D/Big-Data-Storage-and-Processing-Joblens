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
