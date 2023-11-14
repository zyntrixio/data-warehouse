/*
Created by:         Anand Bhakta
Created date:       2023-11-14
Last modified by:
Last modified date:

Description:
        This query is used to create the forecasts for transaction metrics by retailer.
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
        BRAND,
        SPEND AS t073__spend__daily_channel_brand_retailer__sum__forecast,
        sum(SPEND) over (
            partition by loyalty_plan_company, loyalty_plan_name, channel
            order by date asc
        ) as t067__spend__daily_channel_brand_retailer__csum__forecast
    FROM
        lc_events
)

SELECT * FROM lc_select
