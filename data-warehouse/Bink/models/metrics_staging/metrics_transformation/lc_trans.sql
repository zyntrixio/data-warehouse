/*
Created by:         Anand Bhakta
Created date:       2023-05-05
Last modified by:   2023
Last modified date: 

Description:
    Rewrite of the LL table lc_joins_links_snapshot and lc_joins_links containing both snapshot and daily absolute data of all link and join journeys split by merchant.
Notes:
    This code can be made more efficient if the start is pushed to the trans__lbg_user code and that can be the source for the majority of the dashboards including user_loyalty_plan_snapshot and user_with_loyalty_cards
Parameters:
    source_object       - src__fact_lc
                        - src__dim_date
*/

WITH lc_events AS (
    SELECT *
    FROM {{ref('stg_metrics__fact_lc')}}
)

,transforming_deletes AS (
    SELECT
        EVENT_DATE_TIME
        ,USER_ID
        ,CHANNEL
        ,BRAND
        ,COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID)    AS USER_REF
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,EVENT_TYPE
        ,CASE EVENT_TYPE
            WHEN 'REMOVED' THEN LAG(AUTH_TYPE, 1) OVER (PARTITION BY USER_REF, LOYALTY_CARD_ID ORDER BY EVENT_DATE_TIME ASC)
            ELSE AUTH_TYPE
            END AS AUTH_TYPE
        ,LAG(EVENT_TYPE, 1) OVER (PARTITION BY USER_REF, LOYALTY_CARD_ID ORDER BY EVENT_DATE_TIME ASC) AS PREV_EVENT
        ,CONSENT_SLUG
        ,CONSENT_RESPONSE
    FROM lc_events
    QUALIFY
        NOT (EVENT_TYPE = 'REMOVED' AND PREV_EVENT != 'SUCCESS')
)

,to_from_dates AS (
    SELECT
        USER_ID
        ,CHANNEL
        ,BRAND
        ,USER_REF
        ,LOYALTY_CARD_ID
        ,COALESCE(
            CASE WHEN auth_type IN ('ADD AUTH', 'AUTH') THEN 'LINK' END,
            CASE WHEN auth_type IN ('JOIN', 'REGISTER') THEN 'JOIN' END
                ) AS ADD_JOURNEY
        ,EVENT_TYPE
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,EVENT_TYPE AS FROM_EVENT
        ,EVENT_DATE_TIME AS FROM_DATE
        ,COALESCE(
            LEAD(EVENT_DATE_TIME, 1) OVER (PARTITION BY USER_REF, LOYALTY_PLAN_NAME ORDER BY EVENT_DATE_TIME)
            ,CURRENT_TIMESTAMP
         ) AS TO_DATE
        ,CONSENT_SLUG
        ,CONSENT_RESPONSE
    FROM
    transforming_deletes
)

select * from to_from_dates
