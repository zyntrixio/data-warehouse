with
ref_file as (select * from {{ source("RETAILER_FORECAST", "RETAILER_FORECAST") }}
),

ref_select as (
    select
        // DAY,
        DATE,
        // MONTH,
        loyalty_plan_company,
        loyalty_plan_name,
        JOINS,
        ACTIVE_USERS,
        SPEND
    from ref_file
)

select *
from ref_select
