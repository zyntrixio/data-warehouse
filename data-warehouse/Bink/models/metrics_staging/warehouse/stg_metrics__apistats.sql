WITH source AS (
    SELECT * 
    FROM {{ ref('fact_api_response_time') }}
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
