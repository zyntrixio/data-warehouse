with
ref_file as (select * from {{ source("RETAILER_FORECAST", "RETAILER_FORECASTS") }}
),

ref_select as (
    select
        // DAY,
        DATE,
        // MONTH,
        loyalty_plan_company,
        loyalty_plan_name,
        channel,
        null as brand,
        JOINS,
        ACTIVE_USERS,
        SPEND
    from ref_file
)

select *
from ref_select
