/*
Created by:         Christopher Mitchell
Created date:       2023-07-17
Last modified by:   Anand Bhakta
Last modified date: 2024-02-26

Description:
    User table, which relates to the transform date into do date and from date for metrics layer   
    INCREMENTAL STRATEGY: LOADS ALL NEWLY INSERTED RECORDS AND ALL PREVIOUS RECORDS FOR OBJECT WHICH ARE UPDATED,
     TRANSFORMS, THEN MERGING BASED ON THE UNIQUE_KEY

Parameters:
    ref_object      - stg_metrics__fact_transaction
*/

{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
trans_events as (select * from {{ ref("stg_metrics__fact_transaction") }}),

filter_data as (
    select *
    from trans_events
    where
    {% for retailor, dates in var("retailor_live_dates").items() %}
        ((loyalty_plan_company = '{{retailor}}' and date >= '{{dates[0]}}' and date <= '{{dates[1]}}') or loyalty_plan_company != '{{retailor}}')
    {%- if not loop.last %} and {% endif -%}
    {% endfor %}
    {% if is_incremental() %}
            and
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
    {% endif %}
),

transforming_refs as (
    select
        event_id,
        date,
        user_id,
        -- external_user_ref,
        channel,
        brand,
        coalesce(nullif(external_user_ref, ''), user_id) as user_ref,
        transaction_id,
        -- provider_slug,
        duplicate_transaction,
        feed_type,
        loyalty_plan_name,
        loyalty_plan_company,
        transaction_date,
        spend_amount,
        -- loyalty_id,
        -- merchant_id, 
        -- payment_account_id,
        loyalty_card_id,
        inserted_date_time
    from filter_data
),

txn_flag as (
    select
        *,
        case
            when duplicate_transaction
                then 'DUPLICATE'
            when
                loyalty_plan_company = 'Viator'
                and (spend_amount = 1 or spend_amount = -1)
                then 'BNPL'
            when spend_amount > 0
                then 'TXNS'
            when spend_amount < 0
                then 'REFUND'
            else 'OTHER'
        end as status,
        sysdate() as updated_date_time
    from transforming_refs
)

select *
from txn_flag
