/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   
Last modified date: 

Description:
    Transaction table from the transformation layer, with nulls removed

Parameters:
    ref_object      - transformed_transactions
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
		,COALESCE(USER_ID, 'NOT_APPLICABLE') AS USER_ID
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
		,COALESCE(BRAND_ID, 'NOT_APPLICABLE') AS BRAND_ID
		,COALESCE(STORE_ID, 'NOT_APPLICABLE') AS STORE_ID
		,FEED_TYPE
		,COALESCE(SCHEME_TRANSACTION_ID, 'NOT APPLICABLE') AS SCHEME_TRANSACTION_ID
		,COALESCE(MERCHANT_IDENTIFIER_ID, 'NOT APPLICABLE') AS MERCHANT_IDENTIFIER_ID
		,COALESCE(PAYMENT_TRANSACTION_ID, 'NOT APPLICABLE') AS PAYMENT_TRANSACTION_ID
	FROM
		transformed_transactions
)

SELECT
    *
FROM
    select_transactions