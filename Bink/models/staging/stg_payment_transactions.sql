/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   
Last modified date: 

Description:
    Stages the user table, containing data about current users

Parameters:
    source_object      - Harmonia.payment_transaction
*/

WITH
transactions as (
	SELECT	*
	FROM {{ source('Harmonia', 'payment_transaction') }}
)

,select_transactions as (
	SELECT
		ID
		,STATUS
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
		,TRANSACTION_ID
		,SPEND_MULTIPLIER
		,TRANSACTION_DATE
		,MERCHANT_IDENTIFIER_IDS

	FROM
		transactions		
)

SELECT
	*
FROM
	select_transactions