/*
Created by:         Christopher Mitchell
Created date:       2023-07-03
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    User transaction metrics monthly by channel, brand and retailera
Parameters:
    source_object       - stg_metrics__fact_transaction
*/
with
user_events as (select * from {{ ref("stg_metrics__fact_transaction") }}),

metrics as (
    select
        date(date_trunc('month', date)) as date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        coalesce(
            nullif(external_user_ref, ''), user_id
        ) as u109__active_users__monthly_channel_brand_retailer__dcount_uid
    from user_events
),

agg as (
    select
        date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        count(
            distinct u109__active_users__monthly_channel_brand_retailer__dcount_uid
        ) as u109__active_users__monthly_channel_brand_retailer__dcount_uid
    from metrics
    group by date, channel, brand, loyalty_plan_company, loyalty_plan_name
)

select *
from agg
