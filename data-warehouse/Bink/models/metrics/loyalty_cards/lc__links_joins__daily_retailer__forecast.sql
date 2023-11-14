/*
Created by:         Anand Bhakta
Created date:       2023-11-14
Last modified by:
Last modified date:

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
        JOINS AS lc047__successful_loyalty_card_joins__daily_retailer__count__forecast,
        sum(JOINS) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as lc067__successful_loyalty_card_joins__daily_retailer__csum__forecast
    FROM
        lc_events
)

SELECT * FROM lc_select
