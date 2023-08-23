/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   Sam Pibworth
Last modified date: 2022-04-22

Description:
    The DIM user table, relating to hermes.user

Parameters:
    ref_object      - stg_hermes__user
*/
{{ config(alias="dim_user") }}

with
    users as (select * from {{ ref("stg_hermes__USER") }}),
    users_select as (
        select
            -- u.user_id,
            uid,
            external_id,
            channel_id,
            -- date_joined,
            delete_token,
            email,
            is_active,
            is_staff,
            is_superuser,
            is_tester,
            -- last_login,
            -- password,
            reset_token,
            marketing_code_id,
            salt,
            apple,
            facebook,
            twitter,
            magic_link_verified  -- Not sure what this is
        from users u
    ),
    users_na_unions as (
        select
            'NOT_APPLICABLE' as user_id,
            null as external_id,
            null as channel_id,
            null as date_joined,
            null as email,
            null as is_active,
            null as is_staff,
            null as is_superuser,
            null as is_tester,
            null as last_login,
            null as marketing_code_id,
            null as salt,
            null as apple,
            null as facebook,
            null as twitter,
            null as magic_link_verified
        union all
        select *
        from users_select
    )

select *
from users_na_unions
