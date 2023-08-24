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
user_events as (select * from {{ ref("stg_metrics__fact_transaction") }}),

metrics as (
    select
        date(date) as date,
        channel,
        brand,
        loyalty_plan_company,
        coalesce(
            nullif(external_user_ref, ''), user_id
        ) as u007__active_users__user_level_daily__uid
    from user_events
    group by
        coalesce(nullif(external_user_ref, ''), user_id),
        channel,
        brand,
        loyalty_plan_company,
        date(date)
)

select *
from metrics
