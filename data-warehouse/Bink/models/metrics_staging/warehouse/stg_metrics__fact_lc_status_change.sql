{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
source as (select * from {{ ref("fact_loyalty_card_status_change_secure") }}
            {% if is_incremental() %}
            where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
            {% endif %}),

renamed as (
    select
        event_id,
        event_date_time,
        loyalty_card_id,
        loyalty_plan_id,
        loyalty_plan_name,
        loyalty_plan_company,
        from_status_id,
        from_status,
        to_status_id,
        to_status,
        is_most_recent,
        origin,
        channel,
        brand,
        external_user_ref,
        user_id,
        email_domain,
        inserted_date_time,
        updated_date_time
    from source
)

select *
from renamed
