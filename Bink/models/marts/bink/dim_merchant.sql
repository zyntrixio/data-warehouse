/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:   
Last modified date: 

Description:
	Dim table for merchants

Parameters:
    ref_object      - stg_harmonia__merchant_identifier
*/

WITH
merchant_identifier AS (
    SELECT * 
    FROM {{ref('stg_harmonia__merchant_identifier')}}
)

,payment_provider AS (
	SELECT * 
    FROM {{ref('stg_harmonia__payment_provider')}}
)

,loyalty_scheme AS (
	SELECT * 
    FROM {{ref('stg_harmonia__loyalty_scheme')}}
)
,merchant_select AS (
SELECT
    m.MERCHANT_ID
    ,m.LOCATION
    ,m.POSTCODE
    ,m.CREATED_AT
    ,m.UPDATED_AT
    ,m.LOCATION_ID
    ,m.LOYALTY_SCHEME_ID
	,l.slug AS LOYALTY_SCHEME_SLUG
    ,CASE WHEN array_contains('visa'::variant, array_agg(p.slug))
		THEN true 
		ELSE false
		END AS payment_provider_visa
	,CASE WHEN array_contains('mastercard'::variant, array_agg(p.slug))
		THEN true 
		ELSE false
		END AS payment_provider_mastercard
	,CASE WHEN array_contains('amex'::variant, array_agg(p.slug))
		THEN true 
		ELSE false
		END AS payment_provider_amex
    ,m.MERCHANT_INTERNAL_ID
FROM
	merchant_identifier m
LEFT JOIN payment_provider p
	ON m.PAYMENT_PROVIDER_ID = p.id
LEFT JOIN loyalty_scheme l
	ON m.LOYALTY_SCHEME_ID = l.id
GROUP BY
    m.MERCHANT_ID
    ,m.LOCATION
    ,m.POSTCODE
    ,m.CREATED_AT
    ,m.UPDATED_AT
    ,m.LOCATION_ID
    ,m.LOYALTY_SCHEME_ID
    ,m.MERCHANT_INTERNAL_ID
	,l.slug
)


SELECT *
FROM merchant_select