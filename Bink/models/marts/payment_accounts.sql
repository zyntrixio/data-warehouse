/*
Created by:         Sam Pibworth
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
	Processes json values and finishes payment_account table

Parameters:
    ref_object      - transformed_payment_accounts
*/

WITH
payment_accounts AS (
    SELECT *
    FROM {{ ref('transformed_payment_accounts')}}
)


,payment_account_select AS (
	SELECT
		ID AS PAYMENT_ACCOUNT_ID
		,HASH
		,TOKEN
		,STATUS
		,PROVIDER_ID
		,PROVIDER_STATUS_CODE
		,COUNTRY -- GB And UK???
		,CREATED
		,PAN_END
		,UPDATED
		,CASE WHEN CONSENTS IN ('[]', '[{}]') -- Need to check this only has 1 entity in array
            THEN NULL
            ELSE PARSE_JSON(CONSENTS)[0]:type :: INT
            END AS CONSENTS_TYPE
        ,CASE WHEN CONSENTS IN ('[]', '[{}]')
            THEN NULL
            ELSE PARSE_JSON(CONSENTS)[0]:timestamp :: TIMESTAMP
            END AS CONSENTS_TIMESTAMP
        ,CASE WHEN CONSENTS IN ('[]', '[{}]')
            THEN NULL
            ELSE PARSE_JSON(CONSENTS)[0]:longitude :: FLOAT
            END AS CONSENTS_LONGITUDE
        ,CASE WHEN CONSENTS IN ('[]', '[{}]')
            THEN NULL
            ELSE PARSE_JSON(CONSENTS)[0]:latitude :: FLOAT
            END AS CONSENTS_LATITUDE
        ,ISSUER_ID
		,PAN_START
		,CASE WHEN PLL_LINKS = '[]'
            THEN NULL
            ELSE PARSE_JSON(PLL_LINKS)[0]:id :: VARCHAR
            END AS PLL_LINK_ID -- Need to check multiple pll links don't exist. If they do we need another table
		,PSP_TOKEN
		,CASE WHEN AGENT_DATA = '{}'
            THEN NULL
            ELSE PARSE_JSON(AGENT_DATA):card_uid :: VARCHAR
            END AS CARD_UID
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
		,CARD_NAME -- Need to check no conflict with the status join
		,CARD_TYPE
		--,FORMATTED_IMAGES -- Complicated JSON - should this be unpacked?
	FROM
		payment_accounts
)
SELECT
    *
FROM
    payment_account_select