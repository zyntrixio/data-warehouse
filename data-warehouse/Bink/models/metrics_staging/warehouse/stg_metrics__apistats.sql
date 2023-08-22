with
    source as (select * from {{ ref("fact_api_response_time") }}),
    renamed as (
        select
            api_id, date_time as date, method, path, channel, response_time, status_code
        from source
    )

select *
from renamed
