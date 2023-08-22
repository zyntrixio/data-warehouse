/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   
Last modified date: 

Description:
    Stages the user table, containing data about current users

Parameters:
    source_object      - HERMES.USER
*/
with
    current_users as (select * from {{ source("Hermes", "USER") }}),
    current_users_renamed as (
        select
            id::varchar as user_id,
            uid,
            external_id,
            client_id::varchar as channel_id,
            date_joined::datetime as date_joined,
            delete_token,
            email,
            is_active,
            is_staff,
            is_superuser,
            is_tester,
            last_login::datetime as last_login,
            marketing_code_id,
            password,
            reset_token,
            salt,
            apple,
            facebook,
            twitter,
            magic_link_verified::datetime as magic_link_verified
        from current_users
    )

select *
from current_users_renamed
