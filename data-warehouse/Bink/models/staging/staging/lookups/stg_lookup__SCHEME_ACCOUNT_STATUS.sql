with
account_status as (select * from {{ source("LOOKUP", "ACCOUNT_STATUS") }}),

account_status_select as (
    select
        code,
        status,
        status_group,
        journey_type,
        status_type,
        status_rollup,
        api2_status,
        api2_error_slug
    from account_status
)

select *
from account_status_select
