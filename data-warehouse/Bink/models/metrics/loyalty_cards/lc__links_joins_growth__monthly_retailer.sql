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
with metrics as (select * from {{ ref("lc__links_joins__monthly_retailer") }})

,lag as (
    select
        date,
        loyalty_plan_name,
        loyalty_plan_company,
        lag(lc335__successful_loyalty_cards__monthly_retailer__pit) over (partition by loyalty_plan_name, loyalty_plan_company order by date) lc335__successful_loyalty_cards__monthly_retailer__pit_prev,
        lc335__successful_loyalty_cards__monthly_retailer__pit
    from
        metrics
)

,growth as (
    select
        date,
        loyalty_plan_name,
        loyalty_plan_company,    
        DIV0(1 - lc335__successful_loyalty_cards__monthly_retailer__pit, lc335__successful_loyalty_cards__monthly_retailer__pit_prev) as growth
    from
        lag
)


select * from growth
