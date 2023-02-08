/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:   
Last modified date: 

Description:
    Stages the loyalty_scheme table

Parameters:
    sources   - harmonia.loyalty_scheme

*/

WITH source AS (
    SELECT
        ID
        ,SLUG::VARCHAR AS SLUG
        ,CREATED_AT
        ,UPDATED_AT
    FROM {{source('HARMONIA','LOYALTY_SCHEME')}}
)


SELECT *
FROM source 