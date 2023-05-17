/*
Created by:         Sam Pibworth
Created date:       2022-06-14
Last modified by:   
Last modified date: 

Description:
	Dim payment account with reduced columns

Parameters:
    ref_object      - dim_payment_account_secure
*/

WITH
payment_account AS (
    SELECT * 
    FROM {{ref('dim_payment_account_secure')}}
)

, payment_account_select AS (
    SELECT
        PAYMENT_ACCOUNT_ID
        // ,HASH
        // ,TOKEN
        ,STATUS
        ,PROVIDER_ID
        ,PROVIDER_STATUS_CODE
        ,COUNTRY
        ,CREATED
        ,PAN_END
        ,UPDATED
        ,CONSENTS_TYPE
        ,CONSENTS_TIMESTAMP
        ,CONSENTS_LONGITUDE
        ,CONSENTS_LATITUDE
        ,ISSUER_ID
        ,PAN_START
        // ,PSP_TOKEN
        // ,CARD_UID
        ,IS_DELETED
        ,START_MONTH
        ,START_YEAR
        // ,EXPIRY_MONTH
        // ,EXPIRY_YEAR
        // ,FINGERPRINT
        ,ISSUER_NAME
        // ,NAME_ON_CARD
        ,CARD_NICKNAME
        ,CURRENCY_CODE
        ,CARD_NAME
        ,CARD_TYPE
    FROM
        payment_account
)


SELECT *
FROM payment_account_select 