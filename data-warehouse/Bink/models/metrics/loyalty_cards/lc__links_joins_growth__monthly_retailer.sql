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

{% set categorical = ["date", "loyalty_plan_name", "loyalty_plan_company"] %}
{% set exclusion = [] %}
{% set partition = ["loyalty_plan_name", "loyalty_plan_company"] %}
{% set order = ["date"] %}

with metrics as (select * from {{ ref("lc__links_joins__monthly_retailer") }})

,lag as (
    select
        {% for col in adapter.get_columns_in_relation(ref('lc__links_joins__monthly_retailer')) if col.column in categorical -%}

        {{ col.column}},

        {% else %}

        div0({{ col.column}} - 1, lag({{ col.column}}) over (partition by loyalty_plan_name, loyalty_plan_company order by date)),

        {% endfor %}
)

select * from lag
where loyalty_plan_company = 'The Works';
