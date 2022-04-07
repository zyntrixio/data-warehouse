with final as (
    select * from {{ ref('stg_user') }}
)

select * from final