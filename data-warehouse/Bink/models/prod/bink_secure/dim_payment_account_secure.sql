/*
Created by:         Sam Pibworth
Created date:       2022-04-21
Last modified by:
Last modified date:

Description:
	Processes json values and finishes payment_account table

Parameters:
    ref_object      - transformed_payment_accounts
*/
{{ config(alias="dim_payment_account") }}

with
payment_accounts as (select * from {{ ref("transformed_payment_accounts") }}),

payment_account_select as (
    select
        payment_account_id,
        hash,
        token,
        status,
        provider_id,
        provider_status_code,
        country,  -- GB And UK???
        created,
        pan_end,
        updated,
        case
            -- Need to check this only has 1 entity in array
            when consents in ('[]', '[{}]')
                then null
            else parse_json(consents)[0]:type::int
        end as consents_type,
        case
            when consents in ('[]', '[{}]')
                then null
            else parse_json(consents)[0]:timestamp::timestamp
        end as consents_timestamp,
        case
            when consents in ('[]', '[{}]')
                then null
            else parse_json(consents)[0]:longitude::float
        end as consents_longitude,
        case
            when consents in ('[]', '[{}]')
                then null
            else parse_json(consents)[0]:latitude::float
        end as consents_latitude,
        issuer_id,
        pan_start,
        psp_token,
        case
            when agent_data = '{}'
                then null
            else parse_json(agent_data):card_uid::varchar
        end as card_uid,
        is_deleted,
        start_month,
        start_year,
        expiry_month,
        expiry_year,
        fingerprint,
        issuer_name,
        name_on_card,
        card_nickname,
        currency_code,
        card_name,  -- Need to check no conflict with the status join
        card_type
        -- ,FORMATTED_IMAGES -- Complicated JSON - should this be unpacked?
    from payment_accounts
),

add_na_value as (
    select
        'NOT_APPLICABLE' as payment_account_id,
        null as hash,
        null as token,
        null as status,
        null as provider_id,
        null as provider_status_code,
        null as country,
        null as created,
        null as pan_end,
        null as updated,
        null as consents_type,
        null as consents_timestamp,
        null as consents_longitude,
        null as consents_latitude,
        null as issuer_id,
        null as pan_start,
        null as psp_token,
        null as card_uid,
        null as is_deleted,
        null as start_month,
        null as start_year,
        null as expiry_month,
        null as expiry_year,
        null as fingerprint,
        null as issuer_name,
        null as name_on_card,
        null as card_nickname,
        null as currency_code,
        null as card_name,  -- Need to check no conflict with the status join
        null as card_type
        -- ,NULL AS FORMATTED
    union all
    select *
    from payment_account_select
)

select *
from add_na_value
