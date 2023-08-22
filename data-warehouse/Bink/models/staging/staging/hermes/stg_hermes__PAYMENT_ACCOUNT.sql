/*
Created by:         Sam Pibworth
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the payment account table

Parameters:
    source_object      - HERMES.PAYMENT_CARD_PAYMENTCARDACCOUNT
*/
with
    payment_account as (
        select
            id::varchar as payment_account_id,
            hash,
            token,
            status,
            country,
            created,
            pan_end,
            updated,
            consents,
            issuer_id,
            pan_start,
            pll_links,
            psp_token,
            agent_data,
            is_deleted,
            start_year,
            expiry_year,
            fingerprint,
            issuer_name,
            start_month,
            expiry_month,
            name_on_card,
            card_nickname,
            currency_code,
            payment_card_id,
            formatted_images
        from {{ source("Hermes", "PAYMENT_ACCOUNT") }}
    )

select *
from payment_account
