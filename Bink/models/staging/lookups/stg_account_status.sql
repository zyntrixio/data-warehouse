WITH account_status AS (
    SELECT *
    FROM {{ref('lookup__account_status')}}
)

,account_status_select AS (
    SELECT
        "Code" AS CODE
        ,"Status" AS STATUS
        ,"Status Group" AS STATUS_GROUP
        ,"Journey Type" AS JOURNEY_TYPE
        ,"Status Type" AS STATUS_TYPE
        ,"Status Rollup" AS STATUS_ROLLUP
    FROM account_status
)

SELECT *
FROM account_status_select
