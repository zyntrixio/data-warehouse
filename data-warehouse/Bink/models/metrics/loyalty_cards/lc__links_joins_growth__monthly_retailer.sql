/*
Created by:         Anand Bhakta
Created date:       2023-09-20
Last modified by:
Last modified date:

Description:
    todo
Parameters:
    source_object       - lc__links_joins__monthly_retailer
*/

{# {% for col in adapter.get_columns_in_relation(ref('lc__links_joins__monthly_retailer')) -%}
    ... {{ col.column }} ...
{% endfor %}
#}

{% set node = "lc__links_joins__monthly_retailer" %}
{% set categorical = ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"] %}
{% set exclusion = [] %}
{% set partition = ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"] %}
{% set order = "DATE" %}

with metrics as (select * from {{ ref(node) }})

,lag as (
    select
        {%- for col in adapter.get_columns_in_relation(ref(node)) -%}
        {%- if col.column in categorical %}
        {{ col.column}},
        {%- else %}
        div0({{ col.column}} - 1, lag({{ col.column}}) over (partition by {% for col in partition %} {{col}} {%- if not loop.last %} , {% endif -%} {% endfor %} order by {{order}}))
        {%- if not loop.last %} , {% endif -%}
        {%- endif %}
        {%- endfor %}
    from
        metrics
)

select * from lag
where loyalty_plan_company = 'The Works';
