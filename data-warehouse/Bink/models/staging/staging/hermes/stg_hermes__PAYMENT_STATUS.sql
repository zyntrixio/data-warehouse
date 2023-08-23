/*
Created by:         Sam Pibworth
Created date:       2022-04-21
Last modified by:
Last modified date:

Description:
    Stages the payment status table

Parameters:
    source_object      - HERMES.PAYMENT_CARD_PROVIDERSTATUSMAPPING
*/
with
payment_status as (
    select
        id,
        provider_id,
        bink_status_code,
        provider_status_code
    from {{ source("Hermes", "PAYMENT_STATUS") }}
)

select *
from payment_status
