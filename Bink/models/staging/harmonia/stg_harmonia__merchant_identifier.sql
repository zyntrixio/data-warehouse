/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:   
Last modified date: 

Description:
    Stages the merchant_identifier table

Parameters:
    sources   - harmonia.merchant_identifier

*/

WITH source AS (
    SELECT
        ID
        ,MID AS MERCHANT_ID
        ,LOCATION
        ,POSTCODE
        ,CREATED_AT::DATETIME AS CREATED_AT
        ,UPDATED_AT::DATETIME AS UPDATED_AT
        ,LOCATION_ID
        ,LOYALTY_SCHEME_ID
        ,PAYMENT_PROVIDER_ID
        ,MERCHANT_INTERNAL_ID
    FROM {{source('HARMONIA','MERCHANT_IDENTIFIER')}}
)


SELECT *
FROM source 