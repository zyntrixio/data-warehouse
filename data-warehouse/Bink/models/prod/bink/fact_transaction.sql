/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   Sam Pibworth
Last modified date: 2022-06-08

Description:
    Transaction table from the the hermes events.

Parameters:
    ref_object      - transformed_transactions
*/
{{ config(materialized="incremental", unique_key="EVENT_ID") }}

with
    transaction_events as (
        select *
        from {{ ref("fact_transaction_secure") }}
        {% if is_incremental() %}
        where updated_date_time >= (select max(updated_date_time) from {{ this }})
        {% endif %}
    ),
    select_transactions as (
        select
            event_id,
            event_date_time,
            -- user_id,
            external_user_ref,
            channel,
            brand,
            transaction_id,
            provider_slug,
            feed_type,
            duplicate_transaction,
            loyalty_plan_name,
            loyalty_plan_company,
            transaction_date,
            spend_amount,
            spend_currency,
            loyalty_id,
            loyalty_card_id,
            merchant_id,
            payment_account_id,
            settlement_key,
            auth_code,
            approval_code,
            inserted_date_time,
            updated_date_time
        from transaction_events
    )

select *
from select_transactions
