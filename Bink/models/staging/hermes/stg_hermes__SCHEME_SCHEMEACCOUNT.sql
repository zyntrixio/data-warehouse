/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the base table for scheme_schemeaccount this holds information about loyalty cards

Parameters:
    source_object      - Hermes.SCHEME_SCHEMEACCOUNT
*/

WITH
source  as (
	SELECT	*
	FROM {{ source('Hermes', 'SCHEME_SCHEMEACCOUNT') }}
)

,renaming  as (



SELECT  BALANCES
       ,ID                   AS LOYALTY_CARD_ID
       ,LINK_DATE::timestamp as LINK_DATE
       ,SCHEME_ID            AS LOYALTY_PLAN_ID
       ,_AIRBYTE_AB_ID
       ,_AIRBYTE_SCHEME_SCHEMEACCOUNT_HASHID
       ,JOIN_DATE::timestamp AS JOIN_DATE
       ,CARD_NUMBER
       ,UPDATED::timestamp   AS UPDATED
       ,BARCODE
       ,VOUCHERS
       ,CREATED::timestamp   AS CREATED
       ,"order"              AS ORDERS
       ,TRANSACTIONS
       ,ORIGINATING_JOURNEY
       ,PLL_LINKS
       ,_AIRBYTE_EMITTED_AT
       ,FORMATTED_IMAGES
       ,IS_DELETED
       ,_AIRBYTE_NORMALIZED_AT
FROM source
	
)

SELECT
	*
FROM renaming
