/*
Created by:         Christopher Mitchell
Created date:       2023-06-23
Last modified by:   
Last modified date: 

Description:
    Rewrite of metrics for transactions at a user level, daily agg
Notes:
    will be used in output for LBG as user level
    source_object       - src fact transaction
*/
with
    txn_events as (select * from {{ ref("stg_metrics__fact_transaction") }}),
    metrics as (
        select
            date(date) as date,
            channel,
            brand,
            loyalty_plan_company,
            sum(spend_amount) as t001__spend__user_level_daily__sum,
            coalesce(
                nullif(external_user_ref, ''), user_id
            ) as t002__active_users__user_level_daily__uid,
            count(
                distinct transaction_id
            ) as t003__transactions__user_level_daily__dcount_txn
        from txn_events
        group by
            coalesce(nullif(external_user_ref, ''), user_id),
            channel,
            brand,
            loyalty_plan_company,
            date(date)
    )

select *
from metrics
