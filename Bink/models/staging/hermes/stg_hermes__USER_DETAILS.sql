/*
Created by:         Sam Pibworth
Created date:       2022-06-13
Last modified by:   
Last modified date: 

Description:
    Stages the user_details table

Parameters:
    source_object      - HERMES.USER_DETAILS
*/

WITH
user_details AS (
	SELECT
		*
	FROM
		{{ source('Hermes', 'USER_DETAIL') }}
)

,user_details_renamed AS (
	SELECT
		ID
		,CITY
		,PHONE
		,GENDER
		,REGION
		,COUNTRY
		,USER_ID
		,CURRENCY
		,POSTCODE
		,LAST_NAME
		,PASS_CODE
		,FIRST_NAME
		,DATE_OF_BIRTH
		,NOTIFICATIONS
		,ADDRESS_LINE_1
		,ADDRESS_LINE_2
	FROM
		user_details
)

SELECT
	*
FROM
	user_details_renamed