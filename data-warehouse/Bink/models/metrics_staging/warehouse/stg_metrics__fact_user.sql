with
source as (
    select * from {{ ref("fact_user_secure") }} where event_type is not null
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
