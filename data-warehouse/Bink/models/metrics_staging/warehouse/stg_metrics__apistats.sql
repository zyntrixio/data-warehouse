{{
    config(
        materialized="incremental",
        unique_key="API_ID"
    )
}}

with
source as (select * from {{ ref("fact_api_response_time") }}
            {% if is_incremental() %}
            where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
            {% endif %}),

renamed as (
    select
        api_id,
        date_time as date,
        method,
        path,
        channel,
        response_time,
        status_code,
        inserted_date_time
    from source
)

select *
from renamed
