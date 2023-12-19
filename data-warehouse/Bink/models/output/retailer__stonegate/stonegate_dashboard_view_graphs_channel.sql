/*
Created by:         Anand Bhakta
Created date:       2023-10-19
Last modified by:
Last modified date:

Description:
    Datasource to produce tableau dashboard for Stonegate Group graphs
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
    from {{ ref("lc__links_joins__daily_channel_brand_retailer") }}
    where loyalty_plan_company = 'Stonegate Group'
),

txn_metrics as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__trans__daily_channel_brand_retailer") }}
    where loyalty_plan_company = 'Stonegate Group'
),

lc_metric_forecast as (
    select
        *,
        'JOINS' as category
    from {{ ref("lc__links_joins__daily_channel_brand_retailer__forecast") }}
    where loyalty_plan_company = 'Stonegate Group'
),

txn_metrics_forecast as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__trans__daily_channel_brand_retailer__forecast") }}
    where loyalty_plan_company = 'Stonegate Group'
),

combine_all as (
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        channel,
        brand,
        LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        LC079__SUCCESSFUL_LOYALTY_CARD_LINKS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as T067__SPEND__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM__forecast,
        null as t067__spend__daily_channel_brand_retailer__csum__forecast
    from lc_metric
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        channel,
        brand,
        null as LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as LC079__SUCCESSFUL_LOYALTY_CARD_LINKS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        T067__SPEND__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM__forecast,
        null as t067__spend__daily_channel_brand_retailer__csum__forecast
    from txn_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        channel,
        brand,
        null as LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as LC079__SUCCESSFUL_LOYALTY_CARD_LINKS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as T067__SPEND__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM__forecast,
        null as t067__spend__daily_channel_brand_retailer__csum__forecast
    from lc_metric_forecast
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        channel,
        brand,
        null as LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as LC079__SUCCESSFUL_LOYALTY_CARD_LINKS__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as T067__SPEND__DAILY_CHANNEL_BRAND_RETAILER__CSUM,
        null as LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM__forecast,
        t067__spend__daily_channel_brand_retailer__csum__forecast
    from txn_metrics_forecast
)

select *
from combine_all
