/*
Created by:         Anand Bhakta
Created date:       2024-01-11
Last modified by:
Last modified date:

Description:
    PLL to and from dates to help assign users for multi channel transactions

Parameters:
    ref_object      - stg_hermes__EVENTS
*/
{{ config(materialized="incremental", unique_key="EVENT_ID") }}

with
pll_events as (
    select *
    from {{ ref("stg_hermes__EVENTS") }}
    where event_type = 'pll_link.statuschange'
    {% if is_incremental() %}
        and
            _airbyte_emitted_at
            >= (select max(inserted_date_time) from {{ this }})
    {% endif %}
)

,select_fields as (
    select
        event_id,
        event_date_time,
        json:internal_user_ref as user_id,
        json:scheme_account_id as loyalty_card_id,
        json:payment_account_id as payment_account_id,
        null as to_date,
        json:to_state = 1 as active_link,
        null as inserted_date_time
    from pll_events
)

,union_old_pll_records as (
    select *
    from select_fields
    {% if is_incremental() %}
        union
        select *
        from {{ this }}
        where
            (loyalty_card_id, payment_account_id, user_id) in (
                select loyalty_card_id, payment_account_id, user_id from select_fields
            )
    {% endif %}
)

,from_to_dates as (
    select
        event_id,
        event_date_time as from_date,
        user_id,
        loyalty_card_id,
        payment_account_id,
        coalesce(
            lead(event_date_time, 1) over (
            partition by loyalty_card_id, payment_account_id, user_id
            order by event_date_time asc
        ),
        current_timestamp) as to_date,
        active_link,
        sysdate() as inserted_date_time
    from union_old_pll_records
    qualify active_link
)

select * from from_to_dates
