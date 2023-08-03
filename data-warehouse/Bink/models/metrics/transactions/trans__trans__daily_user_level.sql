/*
Created by:         Christopher Mitchell
Created date:       2023-06-23
Last modified by:   
Last modified date: 

Description:
    Rewrite of metrics for transactions at a user level, daily agg
Notes:
    will be used in output for LBG as user level
    source_object       - src fact transaction
*/

WITH txn_events AS (
    SELECT *
    FROM {{ref('stg_metrics__fact_transaction')}}
)

,metrics AS (
    SELECT
        DATE(DATE)                                          AS DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,SUM(SPEND_AMOUNT)                                  AS T001__SPEND__USER_LEVEL_DAILY__SUM
        ,COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID)    AS T002__ACTIVE_USERS__USER_LEVEL_DAILY__UID
        ,COUNT(DISTINCT TRANSACTION_ID)                     AS T003__TRANSACTIONS__USER_LEVEL_DAILY__DCOUNT_TXN
    FROM
        txn_events
    GROUP BY
        COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID)
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,DATE(DATE)
)

select * from metrics