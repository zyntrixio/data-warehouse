/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   Anand Bhakta
Last modified date: 2023-01-11

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

pll_mc as (select * from {{ ref("transformed_pll")}}),

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
),

add_mc_users as (
    select 
        t.event_id,
        t.event_date_time,
        t.user_id,
        t.external_user_ref,
        t.channel,
        t.brand,
        t.transaction_id,
        t.provider_slug,
        t.feed_type,
        t.duplicate_transaction,
        t.loyalty_plan_name,
        t.loyalty_plan_company,
        t.transaction_date,
        t.spend_amount,
        t.spend_currency,
        t.loyalty_id,
        t.loyalty_card_id,
        t.merchant_id,
        t.payment_account_id,
        p.ARRAY_AGG(p.user_id) alt_user_id,
        t.settlement_key,
        t.auth_code,
        t.approval_code,
        t.inserted_date_time,
        t.updated_date_time
    from select_transactions t
    left join pll_mc p on p.loyalty_card_id = t.loyalty_card_id and p.payment_account_id = t.payment_account_id
    and t.transaction_date < p.to_date and t.transaction_date >= from_date and p.user_id != t.user_id
    GROUP BY ALL
)

select *
from add_mc_users
