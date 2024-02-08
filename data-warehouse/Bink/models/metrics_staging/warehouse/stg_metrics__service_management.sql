{{
    config(
        materialized="incremental",
        unique_key="ID"
    )
}}

with
source as (select * from {{ ref("service_management") }}
            {% if is_incremental() %}
            where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
            {% endif %}),

renamed as (
    select
        id,
        ticket_id,
        mi,
        status,
        channel,
        service,
        created_at as date,
        updated_at,
        sla_breached,
        is_most_recent,
        -- _airbyte_emitted_at,
        inserted_date_time
    from source
)

select *
from renamed
