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
matched_transactions as (
    SELECT *
    FROM {{ ref('stg_matched_transactions')}}
)

,scheme_transactions as (
    SELECT *
    FROM {{ ref('stg_scheme_transactions')}}
)

,joined_transactions as (
	SELECT
		-- ID
		-- ,s.STATUS
		-- ,m.CARD_TOKEN
		-- ,s.CREATED_AT
		-- ,s.UPDATED_AT
		-- ,s.EXTRA_FIELDS
		-- ,s.SPEND_AMOUNT
		-- ,m.MATCHING_TYPE
		-- ,s.SPEND_CURRENCY
		-- ,s.TRANSACTION_ID
		-- ,s.SPEND_MULTIPLIER
		-- ,s.TRANSACTION_DATE
		-- ,m.SCHEME_TRANSACTION_ID
		-- ,s.MERCHANT_IDENTIFIER_ID
		-- ,s.PAYMENT_TRANSACTION_ID
		-- ,s.HAS_TIME
		-- ,s.AUTH_CODE
		-- ,s.FIRST_SIX
		-- ,s.LAST_FOUR
		-- ,s.MATCH_GROUP
		-- ,s.PROVIDER_SLUG
		-- ,s.PAYMENT_PROVIDER_SLUG
		-- ,s.MERCHANT_IDENTIFIER_IDS
		s.*
	FROM
		scheme_transactions  s
	LEFT JOIN
		matched_transactions m
	ON
		s.Transaction_ID = m.Transaction_ID
)

SELECT
    *
FROM
    joined_transactions