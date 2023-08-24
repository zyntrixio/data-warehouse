/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   Christopher Mitchell
Last modified date: 05-06-2023

Description:
    Channel table, which relates to the user_client tables in Hermes, and to channel in events

Parameters:
    ref_object      - stg_hermes__CLIENT_APPLICATION
					- stg_hermes__ORGANISATION
*/
with
client as (select * from {{ ref("stg_hermes__CLIENT_APPLICATION") }}),

orgainsation as (select * from {{ ref("stg_hermes__ORGANISATION") }}),

client_select as (
    select
        c.channel_id,
        c.channel_name,
        -- c.secret,
        c.organisation_id,
        o.organisation_name
    from client c
    left join orgainsation o on c.organisation_id = o.organisation_id
),

client_na_unions as (
    select
        'NOT_APPICABLE' as channel_id,
        null as channel_name,
        -- null as secret,
        null as organisation_id,
        null as organisation_name
    union all
    select *
    from client_select
)

select *
from client_na_unions
