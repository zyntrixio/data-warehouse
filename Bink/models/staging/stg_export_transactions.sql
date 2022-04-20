/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   
Last modified date: 

Description:
    Stages the export_transaction table, containing the final layer of transaction data

Parameters:
    source_object      - Harmonia.export_transaction
*/

WITH
transactions as (
	SELECT	*
	FROM {{ source('Harmonia', 'export_transaction') }}
)

,select_transactions as (
	SELECT
		ID
		,MID
		,STATUS
		,USER_ID
		,BRAND_ID
		,STORE_ID
		,FEED_TYPE
		,CREATED_AT
		,LOYALTY_ID
		,UPDATED_AT
		,CREDENTIALS
		,SPEND_AMOUNT
		,PROVIDER_SLUG
		,SPEND_CURRENCY
		,TRANSACTION_ID
		,TRANSACTION_DATE :: DATETIME AS TRANSACTION_DATE
		,SCHEME_ACCOUNT_ID
		,PAYMENT_CARD_ACCOUNT_ID
	FROM
		transactions		
)

SELECT
	*
FROM
	select_transactions