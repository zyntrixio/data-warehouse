WITH source AS (
    SELECT * 
    FROM {{ source('SERVICE_DATA', 'FACT_API_RESPONSE_TIME') }}
)

,renamed AS (
    SELECT
        API_ID
        ,DATE_TIME AS DATE
        ,METHOD
        ,PATH
        ,CHANNEL
        ,RESPONSE_TIME
        ,STATUS_CODE
    FROM
        source
)

select * 
from renamed
