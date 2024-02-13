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
user as (select * from {{ ref("dim_user_secure") }})

select *
from user
