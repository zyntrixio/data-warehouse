/*
Created by:         Sam Pibworth
Created date:       2022-04-19
Last modified by:   Sam Pibworth
Last modified date: 2022-06-08

Description:
    Transaction table from the the hermes events.

Parameters:
    ref_object      - transformed_transactions
*/

{{
    config(
        materialized='incremental'
		,unique_key='EVENT_ID'
    )
}}

WITH
transaction_events AS (
	SELECT *
	FROM {{ ref('fact_transaction_secure')}}
	{% if is_incremental() %}
  	WHERE UPDATED_DATE_TIME>= (SELECT MAX(UPDATED_DATE_TIME) from {{ this }})
	{% endif %}
)

,select_transactions as (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,USER_ID
		//  ,EXTERNAL_USER_REF
		,CHANNEL
		,BRAND
		,TRANSACTION_ID
		,PROVIDER_SLUG
		,FEED_TYPE
		,LOYALTY_PLAN_NAME
		,LOYALTY_PLAN_COMPANY
		,TRANSACTION_DATE
		,SPEND_AMOUNT
		,SPEND_CURRENCY
		,LOYALTY_ID
		,LOYALTY_CARD_ID
		,MERCHANT_ID
		,PAYMENT_ACCOUNT_ID
		,SETTLEMENT_KEY
		,AUTH_CODE
		,APPROVAL_CODE
		,INSERTED_DATE_TIME
		,UPDATED_DATE_TIME
	FROM
		transaction_events
)

SELECT
    *
FROM
    select_transactions
