/*
Created by:         Christopher Mitchell
Created date:       2023-06-07
Last modified by:
Last modified date:

Description:
    Rewrite of the LL table lc_joins_links_snapshot and lc_joins_links containing both snapshot and daily absolute data of all link and join journeys split by merchant.
Notes:
    This code can be made more efficient if the start is pushed to the trans__lbg_user code and that can be the source for the majority of the dashboards including user_loyalty_plan_snapshot and user_with_loyalty_cards
Parameters:
    source_object       - src__fact_lc_add
                        - src__fact_lc_removed
                        - src__dim_loyalty_card
                        - src__dim_date
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
