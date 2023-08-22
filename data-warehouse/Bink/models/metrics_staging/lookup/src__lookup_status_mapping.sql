with
source as (
    select * from {{ source("STAGING", "STG_LOOKUP__SCHEME_ACCOUNT_STATUS") }}
)

select *
from source
