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

{{ config(alias='dim_user') }}

WITH
users AS (
	SELECT *
	FROM {{ref('stg_hermes__USER')}}
)

,user_details AS (
	SELECT *
	FROM {{ref('stg_hermes__USER_DETAILS')}}
)

,users_select AS (
    SELECT
		u.USER_ID
		,UID
		,EXTERNAL_ID				
		,CHANNEL_ID			
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
		,ud.CITY
		,ud.PHONE
		,ud.GENDER
		,ud.REGION
		,ud.COUNTRY
		,ud.CURRENCY
		,ud.POSTCODE
		,ud.LAST_NAME
		,ud.PASS_CODE
		,ud.FIRST_NAME
		,ud.DATE_OF_BIRTH
		,ud.NOTIFICATIONS
		,ud.ADDRESS_LINE_1
		,ud.ADDRESS_LINE_2
    FROM
		users u
	LEFT JOIN user_details ud
		ON ud.USER_ID = u.USER_ID
)

,users_na_unions AS (
	SELECT
		'NOT_APPLICABLE' AS USER_ID
		,NULL AS UID
		,NULL AS EXTERNAL_ID				
		,NULL AS CHANNEL_ID			
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
		,NULL AS CITY
		,NULL AS PHONE
		,NULL AS GENDER
		,NULL AS REGION
		,NULL AS COUNTRY
		,NULL AS CURRENCY
		,NULL AS POSTCODE
		,NULL AS LAST_NAME
		,NULL AS PASS_CODE
		,NULL AS FIRST_NAME
		,NULL AS DATE_OF_BIRTH
		,NULL AS NOTIFICATIONS
		,NULL AS ADDRESS_LINE_1
		,NULL AS ADDRESS_LINE_2
	UNION ALL
	SELECT *
	FROM users_select
)

SELECT
    *
FROM
    users_na_unions