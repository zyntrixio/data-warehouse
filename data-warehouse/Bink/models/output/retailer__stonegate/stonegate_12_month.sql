/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-15
LAST MODIFIED BY:   
LAST MODIFIED DATE: 

DESCRIPTION:
    DATASOURCE TO PRODUCE TABLEAU DASHBOARD FOR STONEGATE 12 MONTH ROLLING DATASOURCE
PARAMETERS:
    SOURCE_OBJECT       - SRC__RETAILER_LOOKUPS_STONEGATE_METRICS_REF
                        - STONEGATE_DASHBOARD_AGG
*/

with
    unpivot as (
        {{
            dbt_utils.unpivot(
                relation=ref("stonegate_dashboard_agg"),
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
        from {{ ref("src__retailer_lookups_stonegate_metrics_ref") }}
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
