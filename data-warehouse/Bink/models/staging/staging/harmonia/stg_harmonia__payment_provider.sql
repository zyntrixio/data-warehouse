/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:   
Last modified date: 

Description:
    Stages the payment_provider table

Parameters:
    sources   - harmonia.payment_provider

*/
with
    source as (
        select id, slug, created_at, updated_at
        from {{ source("HARMONIA", "PAYMENT_PROVIDER") }}
    )

select *
from source
