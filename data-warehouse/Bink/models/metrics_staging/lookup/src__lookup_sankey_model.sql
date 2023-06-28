WITH source AS (
    SELECT * 
    FROM {{ source('RAW_BINK_LOOKUP', 'SANKEY_MODEL') }}
)

SELECT *
FROM source
