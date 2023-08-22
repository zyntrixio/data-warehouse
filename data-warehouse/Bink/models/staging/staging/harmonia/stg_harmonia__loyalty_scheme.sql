/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:
Last modified date:

Description:
    Stages the loyalty_scheme table

Parameters:
    sources   - harmonia.loyalty_scheme

*/
with
source as (
    select
        id,
        slug::varchar as slug,
        created_at,
        updated_at
    from {{ source("HARMONIA", "LOYALTY_SCHEME") }}
)

select *
from source
