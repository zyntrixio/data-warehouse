/*
Created by:         Sam Pibworth
Created date:       2022-04-20
Last modified by:   
Last modified date: 

Description:
    Unions the payment and export transaction tables, and then joins in data from the matched transactions table

Parameters:
    ref_object      - stg_payment_account
                    - stg_payment_status
*/



WITH
payment_accounts AS (
    SELECT *
    FROM {{ ref('stg_payment_account')}}
)

,payment_status AS (
    SELECT *
    FROM {{ ref('stg_payment_status')}}
)

,payment_card AS (
    SELECT *
    FROM {{ ref('stg_payment_card')}}
)

,joined_payment_accounts AS (
    SELECT
        a.ID
		,a.HASH
		,a.TOKEN
		,a.STATUS
        ,s.PROVIDER_ID
        ,s.PROVIDER_STATUS_CODE
		,a.COUNTRY
		,a.CREATED
		,a.PAN_END
		,a.UPDATED
		,a.CONSENTS
		,a.ISSUER_ID
		,a.PAN_START
		,a.PLL_LINKS
		,a.PSP_TOKEN
		,a.AGENT_DATA
		,a.IS_DELETED
		,a.START_YEAR
		,a.EXPIRY_YEAR
		,a.FINGERPRINT
		,a.ISSUER_NAME
		,a.START_MONTH
		,a.EXPIRY_MONTH
		,a.NAME_ON_CARD
		,a.CARD_NICKNAME
		,a.CURRENCY_CODE
		,c.NAME AS CARD_NAME
        ,c.TYPE AS CARD_TYPE
		,a.FORMATTED_IMAGES
    FROM
        payment_accounts a
    LEFT JOIN
        payment_status s
        ON a.STATUS = s.ID
    LEFT JOIN
        payment_card c
        ON a.PAYMENT_CARD_ID = c.ID

)

SELECT *
FROM joined_payment_accounts