/*
Created by:         Sam Pibworth
Created date:       2022-06-14
Last modified by:   
Last modified date: 

Description:
	Dim payment account with reduced columns

Parameters:
    ref_object      - dim_payment_account_secure
*/
with
    payment_account as (select * from {{ ref("dim_payment_account_secure") }}),
    payment_account_select as (
        select
            -- payment_account_id,
            -- hash,
            token,
            status,
            provider_id,
            provider_status_code,
            country,
            created,
            pan_end,
            updated,
            consents_type,
            consents_timestamp,
            consents_longitude,
            consents_latitude,
            issuer_id,
            -- pan_start,
            -- psp_token,
            card_uid,
            is_deleted,
            start_month,
            -- start_year,
            -- expiry_month,
            -- expiry_year,
            fingerprint,
            -- issuer_name,
            name_on_card,
            card_nickname,
            currency_code,
            card_name,
            card_type
        from payment_account
    )

select *
from payment_account_select
