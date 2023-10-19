/*
Created by:         Christopher Mitchell
Created date:       2023-10-02
Last modified by:   
Last modified date: 

Description:
    Datasource to produce tableau dashboard for Slim Chickens 12 month rolling datasource
Parameters:
    source_object       - src__retailer_lookups_the_works_metrics_ref
*/
with
    unpivot as (
        {{
            dbt_utils.unpivot(
                relation=ref("slim_chickens_dashboard_view"),
                cast_to="number(38,2)",
                exclude=[
                    "date",
                    "category",
                    "loyalty_plan_company",
                    "loyalty_plan_name",
                ],
                field_name="metric",
                value_name="value",
            )
        }}
    ),
    refs as (
        select *
        from {{ ref("src__retailer_lookups_slim_chickens_metrics_ref") }}
        where dashboard = '12_MONTH_ROLLING'
    ),
    rename as (
        select
            date,
            u.category,
            u.loyalty_plan_company,
            u.loyalty_plan_name,
            u.metric,
            r.metric_name,
            u.value
        from unpivot u
        left join refs r on r.metric_ref = u.metric
        where value is not null
    )

select *
from rename
