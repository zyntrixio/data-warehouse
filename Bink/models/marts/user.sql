/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   
Last modified date: 

Description:
    Unions the user and historical_user tables, ensuring that any duplicate IDs are removed. In this instance, the current
    user record is included and the historical record is not.

Parameters:
    ref_object      - transformed_user
    ref_object      - transformed_histuser
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

SELECT
    *
FROM
    users_select