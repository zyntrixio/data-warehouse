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
payment_card as (
    select
        id,
        url,
        name,
        slug,
        type,
        system,
        is_active,
        input_label,
        scan_message,
        token_method,
        formatted_images
    from {{ source("Hermes", "PAYMENT_CARD") }}
)

select *
from payment_card
