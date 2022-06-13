/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the base table the indovdual loyalty plans

Parameters:
    source_object      - Hermes.SCHEME_SCHEME
*/

WITH
source  as (
	SELECT	*
	FROM {{ source('Hermes', 'SCHEME_SCHEME') }}
)

,renaming  as (


SELECT  ID as LOYALTY_PLAN_ID
       ,HAS_POINTS
       ,JOIN_URL
       ,URL
       ,_AIRBYTE_EMITTED_AT
       ,_AIRBYTE_SCHEME_SCHEME_HASHID
       ,COMPANY as LOYALTY_PLAN_COMPANY
       ,ITUNES_URL
       ,AUTHORISATION_REQUIRED
       ,BARCODE_PREFIX
       ,BARCODE_TYPE
       ,CARD_NUMBER_PREFIX
       ,COLOUR
       ,ENROL_INCENTIVE
       ,PLAY_STORE_URL
       ,PLAN_POPULARITY
       ,DIGITAL_ONLY
       ,PLAN_NAME 
       ,SLUG as LOYALTY_PLAN_SLUG
       ,TEXT_COLOUR
       ,TIER as LOYALTY_PLAN_TIER
       ,MAX_POINTS_VALUE_LENGTH
       ,PLAN_DESCRIPTION
       ,PLAN_SUMMARY
       ,FORMATTED_IMAGES
       ,IOS_SCHEME
       ,LINKING_SUPPORT
       ,PLAN_NAME_CARD as LOYALTY_PLAN_NAME_CARD
       ,PLAN_REGISTER_INFO
       ,_AIRBYTE_NORMALIZED_AT
       ,BARCODE_REGEX
       ,COMPANY_URL
       ,JOIN_T_AND_C
       ,NAME as LOYALTY_PLAN_NAME
       ,SCAN_MESSAGE
       ,IDENTIFIER
       ,ANDROID_APP_ID
       ,BARCODE_REDEEM_INSTRUCTIONS
       ,CARD_NUMBER_REGEX
       ,FORGOTTEN_PASSWORD_URL
       ,HAS_TRANSACTIONS
       ,LINK_ACCOUNT_TEXT
       ,POINT_NAME
       ,CATEGORY_ID as LOYALTY_PLAN_CATEGORY_ID
       ,SECONDARY_COLOUR
       ,TRANSACTION_HEADERS
       ,_AIRBYTE_AB_ID
FROM source
	
)

SELECT
	*
FROM renaming