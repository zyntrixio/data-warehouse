/*
Created by:         Christopher Mitchell
Created date:       2023-06-23
Last modified by:   Anand Bhakta
Last modified date: 2024-02-26

Description:
    Daily user level transaction metrics such as active user and transactions per user. 
	INCREMENTAL STRATEGY: LOADS ALL NEWLY INSERTED RECORDS, TRANSFORMS, THEN LOADS
	ALL PREVIOUS RECORDS FOR USERS WHICH ARE UPDATED AND MERGING BASED ON THE UNIQUE_KEY
Notes:
    source_object       - stg_metrics__fact_transaction
*/

{{
    config(
        materialized="table",
        unique_key="UNIQUE_KEY"
    )
}}

with
txn_events as (select * from {{ ref("stg_metrics__fact_transaction") }}
    {% if is_incremental() %}
            where (user_id, date(date)) in 
            (
                select 
                    user_id, date(date)
                from 
                    {{ ref("stg_metrics__fact_transaction") }}
                where 
                    inserted_date_time >= (select max(inserted_date_time) from {{ this }})
                    )
    {% endif %}
),

metrics as (
    select
        date(date) as date,
        channel,
        brand,
        loyalty_plan_company,
        sum(spend_amount) as t001__spend__user_level_daily__sum,
        coalesce(nullif(external_user_ref, ''), user_id) as t002__active_users__user_level_daily__uid,
        count(distinct transaction_id
        ) as t003__transactions__user_level_daily__dcount_txn
    from txn_events
    group by
        coalesce(nullif(external_user_ref, ''), user_id),
        channel,
        brand,
        loyalty_plan_company,
        date(date)
),

finalise as (
    select
        date,
        channel,
        brand,
        loyalty_plan_company,
        t001__spend__user_level_daily__sum,
        t002__active_users__user_level_daily__uid,t003__transactions__user_level_daily__dcount_txn,
        sysdate() as inserted_date_time,
        MD5(date||t002__active_users__user_level_daily__uid) as unique_key
    from metrics
)

select *
from finalise
