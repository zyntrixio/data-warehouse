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

WITH

users AS (
	SELECT *
	FROM {{ref('stg_hermes__user')}}
)

,users_select AS (
    SELECT
		USER_ID
		,UID
		,EXTERNAL_ID				
		,CLIENT_ID			
		,DATE_JOINED
		,DELETE_TOKEN		
		,EMAIL					
		,IS_ACTIVE			
		,IS_STAFF			
		,IS_SUPERUSER		
		,IS_TESTER			
		,LAST_LOGIN			
		,PASSWORD			
		,RESET_TOKEN
		,MARKETING_CODE_ID			
		,SALT
		,APPLE
		,FACEBOOK						
		,TWITTER									
		,MAGIC_LINK_VERIFIED -- Not sure what this is
    FROM
		users
)

,users_na_unions AS (
	SELECT
		'NOT_APPLICABLE' AS USER_ID
		,NULL AS UID
		,NULL AS EXTERNAL_ID				
		,NULL AS CLIENT_ID			
		,NULL AS DATE_JOINED
		,NULL AS DELETE_TOKEN		
		,NULL AS EMAIL					
		,NULL AS IS_ACTIVE			
		,NULL AS IS_STAFF			
		,NULL AS IS_SUPERUSER		
		,NULL AS IS_TESTER			
		,NULL AS LAST_LOGIN			
		,NULL AS PASSWORD			
		,NULL AS RESET_TOKEN
		,NULL AS MARKETING_CODE_ID			
		,NULL AS SALT
		,NULL AS APPLE
		,NULL AS FACEBOOK						
		,NULL AS TWITTER									
		,NULL AS MAGIC_LINK_VERIFIED
	UNION ALL
	SELECT *
	FROM users_select
)

SELECT
    *
FROM
    users_na_unions