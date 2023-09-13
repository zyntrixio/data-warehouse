/*
Created by:         Anand Bhakta
Created date:       2023-09-12
Last modified by:
Last modified date:

Description:
    Datasource to produce tableau dashboard for The Works graphs
Parameters:
    source_object       - lc__links_joins__monthly_retailer
                        - trans__trans__monthly_retailer
                        - trans__avg__monthly_retailer
                        - user__transactions__monthly_retailer
*/
with
lc_metric as (
    select
        *,
        'JOINS' as category
    from {{ ref("lc__links_joins__daily_retailer") }}
    where loyalty_plan_company = 'The Works'
),

txn_metrics as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__trans__daily_retailer") }}
    where loyalty_plan_company = 'The Works'
),

combine_all as (
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        lc067__successful_loyalty_card_joins__daily_retailer__csum,
        null as t027__spend__daily_retailer__csum
    from lc_metric
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc067__successful_loyalty_card_joins__daily_retailer__csum,
        t027__spend__daily_retailer__csum
    from txn_metrics
)

select *
from combine_all
