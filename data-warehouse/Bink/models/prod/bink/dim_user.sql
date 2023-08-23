/*
Created by:         Sam Pibworth
Created date:       2022-06-15
Last modified by:   
Last modified date: 

Description:
	Dim user with reduced columns

Parameters:
    ref_object      - dim_user_secure
*/
with
    user as (select * from {{ ref("dim_user_secure") }}),
    user_select as (
        select
            user_id,
            -- external_id,
            channel_id,
            date_joined,
            -- delete_token,
            -- email,
            is_active,
            is_staff,
            is_superuser,
            is_tester,
            last_login,
            -- password,
            -- reset_token,
            marketing_code_id,
            salt,
            -- apple,
            -- facebook,
            -- twitter,
            magic_link_verified
        from user
    )

select *
from user_select
