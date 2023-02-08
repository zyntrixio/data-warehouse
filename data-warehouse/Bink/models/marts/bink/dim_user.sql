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

WITH
user AS (
    SELECT * 
    FROM {{ref('dim_user_secure')}}
)

, user_select AS (
    SELECT
		USER_ID
		// ,EXTERNAL_ID				
		,CHANNEL_ID			
		,DATE_JOINED
		// ,DELETE_TOKEN		
		// ,EMAIL					
		,IS_ACTIVE			
		,IS_STAFF			
		,IS_SUPERUSER		
		,IS_TESTER			
		,LAST_LOGIN			
		// ,PASSWORD			
		// ,RESET_TOKEN
		,MARKETING_CODE_ID			
		,SALT
		// ,APPLE
		// ,FACEBOOK						
		// ,TWITTER									
		,MAGIC_LINK_VERIFIED
		,CITY
		// ,PHONE
		// ,GENDER
		,REGION
		,COUNTRY
		,CURRENCY
		// ,POSTCODE
		// ,LAST_NAME
		,PASS_CODE
		// ,FIRST_NAME
		// ,DATE_OF_BIRTH
		,NOTIFICATIONS
		// ,ADDRESS_LINE_1
		// ,ADDRESS_LINE_2
    FROM
        user
)


SELECT *
FROM user_select