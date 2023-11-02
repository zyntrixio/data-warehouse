with
ref_file as (select * from {{ source("RETAILER_LOOKUP", "REWARD_RULES") }}
),

ref_select as (
    select
        loyalty_plan_company,
        loyalty_plan_name,
        value,
        reward
    from ref_file
)

select *
from ref_select
