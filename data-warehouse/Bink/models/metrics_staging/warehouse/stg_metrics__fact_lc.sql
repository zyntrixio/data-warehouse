{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
source as (select * from {{ ref("fact_loyalty_card_secure") }}
            {% if is_incremental() %}
            where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
            {% endif %}),

renamed as (
    select
        event_id,
        event_date_time,
        auth_type,
        event_type,
        loyalty_card_id,
        loyalty_plan,
        loyalty_plan_name,
        loyalty_plan_company,
        is_most_recent,
        channel,
        origin,
        brand,
        user_id,
        external_user_ref,
        email_domain,
        consent_slug,
        consent_response,
        inserted_date_time,
        updated_date_time
    from source
)

select *
from renamed
