/*
Created by:         Anand Bhakta
Created date:       2023-11-14
Last modified by: Anand Bhakta
Last modified date: 2023-12-19

Description:
        This query is used to create the forecasts for join metrics by retailer.
Parameters:
    source_object       - src__retailer_forecast
*/
with
lc_events as (select * from {{ ref("src__retailer_forecast") }})

,lc_select as (
    select
        DATE,
        LOYALTY_PLAN_COMPANY,
        loyalty_plan_name,
        channel,
        brand,
        JOINS AS lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count__forecast,
        sum(JOINS) over (
            partition by loyalty_plan_company, loyalty_plan_name, channel
            order by date asc
        ) as LC075__SUCCESSFUL_LOYALTY_CARD_JOINS__DAILY_CHANNEL_BRAND_RETAILER__CSUM__forecast
    FROM
        lc_events
)

SELECT * FROM lc_select
