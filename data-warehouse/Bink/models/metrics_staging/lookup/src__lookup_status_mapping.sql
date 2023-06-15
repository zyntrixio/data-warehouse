WITH source AS (
    SELECT * 
    FROM {{ source('STAGING', 'STG_LOOKUP__SCHEME_ACCOUNT_STATUS') }}
)

SELECT *
FROM source
