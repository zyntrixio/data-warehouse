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

WITH
current_users as (
	SELECT
		*
	FROM
		{{ source('Hermes', 'USER') }}
)

,current_users_renamed as (
	SELECT
		ID :: VARCHAR AS USER_ID
		,UID
		,EXTERNAL_ID				
		,CLIENT_ID :: VARCHAR AS CHANNEL_ID
		,DATE_JOINED :: DATETIME AS DATE_JOINED
		,DELETE_TOKEN		
		,EMAIL					
		,IS_ACTIVE			
		,IS_STAFF			
		,IS_SUPERUSER		
		,IS_TESTER			
		,LAST_LOGIN :: DATETIME AS LAST_LOGIN			
		,MARKETING_CODE_ID	
		,PASSWORD			
		,RESET_TOKEN			
		,SALT
		,APPLE
		,FACEBOOK						
		,TWITTER									
		,MAGIC_LINK_VERIFIED :: DATETIME AS MAGIC_LINK_VERIFIED
	FROM
		current_users
)

SELECT
	*
FROM
	current_users_renamed