with source as (select * from {{ source("RAW_BINK_LOOKUP", "SANKEY_MODEL") }})

select *
from source
