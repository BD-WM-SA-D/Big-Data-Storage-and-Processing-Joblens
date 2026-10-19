{#- Dùng đúng tên schema khai báo (gold, staging) thay vì ghép "<target>_<schema>" mặc định của dbt. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
