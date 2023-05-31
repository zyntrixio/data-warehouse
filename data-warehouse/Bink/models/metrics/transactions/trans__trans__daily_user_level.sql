/*
Created by:         Anand Bhakta
Created date:       2023-02-05
Last modified by:   
Last modified date: 

Description:
    Rewrite of the LL table lc_joins_links_snapshot and lc_joins_links containing both snapshot and daily absolute data of all link and join journeys split by merchant.
Notes:
    This code can be made more efficient if the start is pushed to the trans__lbg_user code and that can be the source for the majority of the dashboards including user_loyalty_plan_snapshot and user_with_loyalty_cards
Parameters:
    source_object       - src__fact_lc_add
                        - src__fact_lc_removed
                        - src__dim_loyalty_card
                        - src__dim_date
*/

WITH trans_events AS (
    SELECT *
    FROM {{ref('src__fact_transaction')}}
)

,metrics AS (
    SELECT
        DATE(DATE) AS DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,SUM(SPEND_AMOUNT) AS SPEND
        ,COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID) AS ACTIVE_USERS
        ,COUNT(TRANSACTION_ID) AS TRANSACTIONS
    FROM
        trans_events
    GROUP BY
        COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID)
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,DATE(DATE)
)

select * from metrics
