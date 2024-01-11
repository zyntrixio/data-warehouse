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
)

,select_fields as (
    select
        event_date_time,
        json:internal_user_ref as user_id,
        json:scheme_account_id as loyalty_card_id,
        json:payment_account_id as payment_account_id,
        json:to_state as to_status
    from pll_events
)

,from_to_dates as (
    select
        user_id,
        loyalty_card_id,
        payment_account_id,
        event_date_time as from_date,
        coalesce(
            lead(event_date_time, 1) over (
            partition by loyalty_card_id, payment_account_id, user_id
            order by event_date_time asc
        ),
        current_timestamp) as to_date,
        to_status = 1 as active_link
    from select_fields
    qualify active_link
)

select * from from_to_dates
