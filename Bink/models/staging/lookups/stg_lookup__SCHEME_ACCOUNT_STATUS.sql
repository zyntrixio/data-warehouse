WITH account_status AS (
    SELECT *
    FROM {{source('LOOKUP','ACCOUNT_STATUS')}}
)

,account_status_select AS (
    SELECT
        CODE
        ,STATUS
        ,STATUS_GROUP
        ,JOURNEY_TYPE
        ,STATUS_TYPE
        ,STATUS_ROLLUP
        ,API2_STATUS
        ,API2_ERROR_SLUG
    FROM account_status
)

SELECT *
FROM account_status_select
