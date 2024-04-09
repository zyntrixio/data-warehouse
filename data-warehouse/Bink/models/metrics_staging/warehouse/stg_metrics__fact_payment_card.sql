{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
source as (
    select * from {{ ref("fact_payment_account_secure") }}
    {% if is_incremental() %}
        where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
    {% endif %}
),

renamed as (
    select
        event_id,
        event_date_time,
        payment_account_id,
        event_type,
        is_most_recent,
        status_id,
        status,
        origin,
        channel,
        brand,
        user_id,
        external_user_ref,
        expiry_month,
        expiry_year,
        expiry_year_month,
        token,
        email,
        email_domain,
        inserted_date_time,
        updated_date_time
    from source
)

select *
from renamed
