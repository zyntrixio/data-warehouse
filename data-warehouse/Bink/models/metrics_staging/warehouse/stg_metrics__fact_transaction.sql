{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
source as (select * from {{ ref("fact_transaction_secure") }}
            {% if is_incremental() %}
            where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
            {% endif %}),

renamed as (
    select
        event_id,
        event_date_time as date,
        user_id,
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
        -- spend_currency,
        loyalty_id,
        loyalty_card_id,
        merchant_id,
        -- settlement_key,
        inserted_date_time,
        updated_date_time,
        payment_account_id
    from source
)

select *
from renamed
