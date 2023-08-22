with
payment_status as (select * from {{ source("LOOKUP", "PAYMENT_STATUS") }}),

payment_status_select as (
    select
        status,
        id as payment_status_id
    from payment_status
)

select *
from payment_status_select
