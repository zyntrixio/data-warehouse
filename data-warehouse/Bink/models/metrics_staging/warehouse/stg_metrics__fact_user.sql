{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
source as (
    select * from {{ ref("fact_user_secure") }} where event_type is not null
            {% if is_incremental() %}
            and
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
            {% endif %}
),

renamed as (
    select
        event_id,
        event_date_time,
        user_id,
        external_user_ref,
        event_type,
        is_most_recent,
        origin,
        channel,
        brand,
        inserted_date_time,
        updated_date_time
    from source
)

select *
from renamed
