/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:   
Last modified date: 

Description:
    Stages the merchant_identifier table

Parameters:
    sources   - harmonia.merchant_identifier

*/
with
    source as (
        select
            id,
            identifier as merchant_id,
            location,
            postcode,
            created_at::datetime as created_at,
            updated_at::datetime as updated_at,
            location_id,
            loyalty_scheme_id,
            payment_provider_id,
            merchant_internal_id
        from {{ source("HARMONIA", "MERCHANT_IDENTIFIER") }}
    )

select *
from source
