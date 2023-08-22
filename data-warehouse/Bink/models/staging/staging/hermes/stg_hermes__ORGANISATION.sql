/*
Created by:         Sam Pibworth
Created date:       2022-04-22
Last modified by:
Last modified date:

Description:
    Stages the organisation table

Parameters:
    source_object      - HERMES.USER_ORGANISATION
*/
with
organisation as (
    select
        id as organisation_id,
        name as organisation_name,
        terms_and_conditions
    from {{ source("Hermes", "ORGANISATION") }}
)

select *
from organisation
