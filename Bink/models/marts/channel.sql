/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   
Last modified date: 

Description:
    Channel table, which relates to the user_client tables in Hermes, and to channel in events

Parameters:
    ref_object      - stg_hermes__CLIENT_APPLICATION
*/

WITH

client AS (
	SELECT *
	FROM {{ref('stg_hermes__CLIENT_APPLICATION')}}
)

,client_select AS (
    SELECT
		CHANNEL_ID
		,CHANNEL_NAME
		,SECRET
		,ORGANISATION_ID
    FROM
		client
)

// ,users_na_unions AS (
// 	SELECT
// 		'NOT_APPLICABLE' AS USER_ID
// 		,NULL AS UID
// 		,NULL AS EXTERNAL_ID				
// 		,NULL AS CLIENT_ID			
// 		,NULL AS DATE_JOINED
// 		,NULL AS DELETE_TOKEN		
// 		,NULL AS EMAIL					
// 		,NULL AS IS_ACTIVE			
// 		,NULL AS IS_STAFF			
// 		,NULL AS IS_SUPERUSER		
// 		,NULL AS IS_TESTER			
// 		,NULL AS LAST_LOGIN			
// 		,NULL AS PASSWORD			
// 		,NULL AS RESET_TOKEN
// 		,NULL AS MARKETING_CODE_ID			
// 		,NULL AS SALT
// 		,NULL AS APPLE
// 		,NULL AS FACEBOOK						
// 		,NULL AS TWITTER									
// 		,NULL AS MAGIC_LINK_VERIFIED
// 	UNION ALL
// 	SELECT *
// 	FROM users_select
// )

SELECT
    *
FROM
    client_select