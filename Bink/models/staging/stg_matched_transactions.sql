/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   
Last modified date: 

Description:
    Stages the user table, containing data about current users

Parameters:
    source_object      - Harmonia.matched_transaction
*/

WITH
transactions as (
	SELECT	*
	FROM {{ source('Harmonia', 'matched_transaction') }}
)

select_transactions as (
	SELECT
		ID
		,STATUS
		,CARD_TOKEN
		,CREATED_AT
		,UPDATED_AT
		,EXTRA_FIELDS
		,SPEND_AMOUNT
		,MATCHING_TYPE
		,SPEND_CURRENCY
		,TRANSACTION_ID
		,SPEND_MULTIPLIER
		,TRANSACTION_DATE
		,SCHEME_TRANSACTION_ID
		,MERCHANT_IDENTIFIER_ID
		,PAYMENT_TRANSACTION_ID
	FROM
		transactions		
)

SELECT
	*
FROM
	select_transactions