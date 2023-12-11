/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   Anand Bhakta
Last modified date: 2023-12-11

Description:
    Transaction table from the the hermes events.

Parameters:
    ref_object      - transformed_transactions
*/
{{
    config(
        alias="fact_transaction", materialized="incremental", unique_key="EVENT_ID"
    )
}}

with
transaction_events as (
    select *
    from {{ ref("transformed_hermes_events") }}
    where
        event_type in ('transaction.exported', 'transaction.duplicate')
        {% if is_incremental() %}
            and _airbyte_normalized_at
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %}
),

loyalty_plan as (select * from {{ ref("stg_hermes__SCHEME_SCHEME") }}),

dim_user as (select * from {{ ref("stg_hermes__USER") }}),

dim_channel as (select * from {{ ref("stg_hermes__CLIENT_APPLICATION") }}),

transaction_events_unpack as (
    select
        event_id,
        event_type,
        event_date_time,
        json:internal_user_ref::varchar as user_id,
        json:transaction_id::varchar as transaction_id,
        json:provider_slug::varchar as provider_slug,
        json:feed_type::varchar as feed_type,
        json:transaction_date::datetime as transaction_date,
        json:spend_amount / 100::number(12, 2) as spend_amount,
        json:spend_currency::varchar as spend_currency,
        json:loyalty_id::varchar as loyalty_id,
        json:scheme_account_id::varchar as loyalty_card_id,
        json:mid::varchar as merchant_id,
        -- json:location_id::varchar as location_id NOTE: joins to harmonia merchant data,
        -- json:merchant_internal_id::varchar as merchant_internal_id NOTE: joins to harmonia merchant data,
        json:payment_card_account_id::varchar as payment_account_id,
        json:settlement_key::varchar as settlement_key,
        json:authorisation_code::varchar as auth_code,
        json:approval_code::varchar as approval_code

    from transaction_events
),

select_transactions as (
    select
        event_id,
        event_date_time,
        t.user_id,
        u.external_id as external_user_ref,
        case
            when c.channel_name in ('Bank of Scotland', 'Lloyds', 'Halifax')
                then 'LLOYDS'
            when c.channel_name = 'Barclays Mobile Banking'
                then 'BARCLAYS'
            when c.channel_name = 'Bink'
                then 'BINK'
            else upper(c.channel_name)
        end as channel,
        case
            when c.channel_name in ('Bink', 'Lloyds', 'Halifax')
                then upper(c.channel_name)
            when c.channel_name = 'Barclays Mobile Banking'
                then 'BARCLAYS'
            when c.channel_name = 'Bank of Scotland'
                then 'BOS'
            else upper(c.channel_name)
        end as brand,
        transaction_id,
        provider_slug,
        feed_type,
        event_type = 'transaction.duplicate' as duplicate_transaction,
        lp.loyalty_plan_name,
        lp.loyalty_plan_company,
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
        sysdate() as inserted_date_time,
        sysdate() as updated_date_time
    from transaction_events_unpack t
    left join loyalty_plan lp on lp.loyalty_plan_slug = t.provider_slug
    left join dim_user u on u.user_id = t.user_id
    left join dim_channel c on u.channel_id = c.channel_id
)

select *
from select_transactions
