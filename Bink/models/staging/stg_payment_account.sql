/*
Created by:         Sam Pibworth
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the payment account table

Parameters:
    source_object      - HERMES.PAYMENT_CARD_PAYMENTCARDACCOUNT
*/

WITH payment_account AS (
	SELECT
		ID
		,HASH
		,TOKEN
		,STATUS
		,COUNTRY
		,CREATED
		,PAN_END
		,UPDATED
		,CONSENTS
		,ISSUER_ID
		,PAN_START
		,PLL_LINKS
		,PSP_TOKEN
		,AGENT_DATA
		,IS_DELETED
		,START_YEAR
		,EXPIRY_YEAR
		,FINGERPRINT
		,ISSUER_NAME
		,START_MONTH
		,EXPIRY_MONTH
		,NAME_ON_CARD
		,CARD_NICKNAME
		,CURRENCY_CODE
		,PAYMENT_CARD_ID
		,FORMATTED_IMAGES
	FROM
		{{ source('Hermes', 'PAYMENT_ACCOUNT') }}
)

SELECT
	*
FROM
	payment_account
	