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
        ,STATUS_V2
        ,ERROR_SLUG_V2
    FROM account_status
)

SELECT *
FROM account_status_select
