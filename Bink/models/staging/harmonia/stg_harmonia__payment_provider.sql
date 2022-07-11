/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:   
Last modified date: 

Description:
    Stages the payment_provider table

Parameters:
    sources   - harmonia.payment_provider

*/

WITH source AS (
    SELECT
        ID
        ,SLUG
        ,CREATED_AT
        ,UPDATED_AT
    FROM {{source('HARMONIA','PAYMENT_PROVIDER')}}
)


SELECT *
FROM source 