/*
Created by:         Sam Pibworth
Created date:       2022-04-22
Last modified by:   
Last modified date: 

Description:
    Stages the client_application table

Parameters:
    source_object      - HERMES.USER_CLIENTAPPLICATION
*/
with
    client_application as (
        select
            client_id::varchar as channel_id,
            name as channel_name,
            secret,
            organisation_id

        from {{ source("Hermes", "CLIENT_APPLICATION") }}
    )

select *
from client_application
