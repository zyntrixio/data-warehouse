/*
Created by:         Sam Pibworth
Created date:       2022-04-20
Last modified by:   
Last modified date: 

Description:
    Unions the payment and export transaction tables, and then joins in data from the matched transactions table

Parameters:
    ref_object      - stg_harmonia__export_transactions
					- stg_harmonia__payment_transactions
					- stg_harmonia__matched_transactions
*/


WITH

export_transactions AS (
	SELECT *
	FROM {{ ref('stg_harmonia__export_transactions')}}
)

,payment_transactions AS (
	SELECT *
	FROM {{ ref('stg_harmonia__payment_transactions')}}
)

,matched_transactions AS (
	SELECT *
	FROM {{ ref('stg_harmonia__matched_transactions')}}
)

,useful_trans_ids AS (
	SELECT DISTINCT TRANSACTION_ID
	FROM export_transactions
	UNION
	SELECT DISTINCT TRANSACTION_ID
	FROM payment_transactions
)

,join_payment_details AS (
 	SELECT
		i.TRANSACTION_ID
		,COALESCE(e.STATUS, p.STATUS) as STATUS
		,e.USER_ID
        ,e.TRANSACTION_DATE
		,p.HAS_TIME
		,p.AUTH_CODE
		,p.FIRST_SIX
		,p.LAST_FOUR
		,p.CARD_TOKEN
		,p.CREATED_AT
		,COALESCE(e.UPDATED_AT, p.UPDATED_AT) as UPDATED_AT
		,p.MATCH_GROUP
		,p.EXTRA_FIELDS
		,p.SPEND_AMOUNT
		,p.PROVIDER_SLUG
		,p.SETTLEMENT_KEY
		,p.SPEND_CURRENCY
		,e.BRAND_ID
        ,e.STORE_ID
        ,e.FEED_TYPE
		,m.SCHEME_TRANSACTION_ID
		,m.MERCHANT_IDENTIFIER_ID
		,m.PAYMENT_TRANSACTION_ID

	FROM
		useful_trans_ids i
	LEFT JOIN
		payment_transactions p
		ON i.TRANSACTION_ID = p.TRANSACTION_ID
	LEFT JOIN
		export_transactions e
        ON i.TRANSACTION_ID = e.TRANSACTION_ID
	LEFT JOIN
		matched_transactions m
        ON i.TRANSACTION_ID = m.TRANSACTION_ID
)

SELECT
	*
FROM
	join_payment_details
	