/*
Created by:         Christopher Mitchell
Created date:       2023-06-07
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    User Transaction metrics daily user level

    source_object       - stg_metrics__fact_transaction
*/
with
user_events as (select * from {{ ref("txns_trans") }}),

metrics as (
    select
        date,
        channel,
        brand,
        loyalty_plan_company,
        user_ref as u007__active_users__user_level_daily__uid
    from user_events
    where status = 'TXNS'
    group by
        user_ref,
        channel,
        brand,
        loyalty_plan_company,
        date
)

select *
from metrics
