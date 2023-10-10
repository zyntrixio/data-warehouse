with source as (select * from {{ source("RAW_BINK_LOOKUP", "TESTERS") }})

select *
from source
