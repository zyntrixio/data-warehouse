{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
source as (select * from {{ ref("fact_pll_link_status_change_secure") }}
            {% if is_incremental() %}
            where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
            {% endif %}),

renamed as (
    select
        event_id,
        event_date_time,
        loyalty_card_id,
        -- loyalty_plan_id,
        loyalty_plan_company,
        loyalty_plan_name,
        payment_account_id,
        from_status,
        to_status,
        channel,
        brand,
        -- from_status_id,
        -- to_status_id,
        -- origin,
        -- is_most_recent,
        inserted_date_time,
        updated_date_time,
        user_id,
        external_user_ref
    from source
)

select *
from renamed
