/*
Created by:         Sam Pibworth
Created date:       2022-04-19
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
transformed_transactions as (
    SELECT *
    FROM {{ ref('transformed_transactions')}}
)

,select_transactions as (
	SELECT
		TRANSACTION_ID
		,STATUS
		,USER_ID
		,TRANSACTION_DATE
		,HAS_TIME	
		,AUTH_CODE	
		,FIRST_SIX	
		,LAST_FOUR	
		,CARD_TOKEN	
		,CREATED_AT	
		,UPDATED_AT
		,MATCH_GROUP
		,EXTRA_FIELDS
		,SPEND_AMOUNT
		,PROVIDER_SLUG
		,SETTLEMENT_KEY
		,SPEND_CURRENCY
		,BRAND_ID
		,STORE_ID
		,FEED_TYPE
		,SCHEME_TRANSACTION_ID
		,MERCHANT_IDENTIFIER_ID
		,PAYMENT_TRANSACTION_ID
	FROM
		transformed_transactions
)

SELECT
    *
FROM
    select_transactions