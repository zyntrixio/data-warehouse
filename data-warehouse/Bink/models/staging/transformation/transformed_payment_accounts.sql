/*
Created by:         Sam Pibworth
Created date:       2022-04-20
Last modified by:
Last modified date:

Description:
    Unions the payment and export transaction tables, and then joins in data from the matched transactions table

Parameters:
    ref_object      - stg_hermes__payment_account
                    - stg_hermes__payment_status
					- stg_hermes__payment_card
*/

{{ config(
  enabled=false
) }}

with
payment_accounts as (select * from {{ ref("stg_hermes__PAYMENT_ACCOUNT") }}),

payment_status as (select * from {{ ref("stg_hermes__PAYMENT_STATUS") }}),

payment_card as (select * from {{ ref("stg_hermes__PAYMENT_CARD") }}),

joined_payment_accounts as (
    select
        a.payment_account_id,
        a.hash,
        a.token,
        a.status,
        s.provider_id,
        s.provider_status_code,
        a.country,
        a.created,
        a.pan_end,
        a.updated,
        a.consents,
        a.issuer_id,
        a.pan_start,
        a.pll_links,
        a.psp_token,
        a.agent_data,
        a.is_deleted,
        a.start_year,
        a.expiry_year,
        a.fingerprint,
        a.issuer_name,
        a.start_month,
        a.expiry_month,
        a.name_on_card,
        a.card_nickname,
        a.currency_code,
        c.name as card_name,
        c.type as card_type,
        a.formatted_images
    from payment_accounts a
    left join payment_status s on a.status = s.id
    left join payment_card c on a.payment_card_id = c.id

)

select *
from joined_payment_accounts
