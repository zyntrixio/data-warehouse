/*
Created by:         Anand Bhakta
Created date:       2023-07-12
Last modified by:
Last modified date: 

Description:
    Set up of error to and from data for loyalty card error statuses excluding pending and active
Parameters:
    source_object       - src__fact_lc_status_change
                        - src__lookup_status_mapping
*/

WITH pll_events AS (
    SELECT *
    FROM {{ref('stg_metrics__pll_link_status_change')}}
)

,from_to_dates AS (
    SELECT
		COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID)    AS USER_REF
        ,CONCAT(COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID), LOYALTY_PLAN_COMPANY)    AS LC_USER_REF
        ,CHANNEL
		,BRAND
		,LOYALTY_CARD_ID
        ,LOYALTY_PLAN_COMPANY
        ,LOYALTY_PLAN_NAME
        ,PAYMENT_ACCOUNT_ID
		,EVENT_DATE_TIME AS FROM_DATE
        ,FROM_STATUS
        ,LEAD(EVENT_DATE_TIME, 1) OVER (PARTITION BY LOYALTY_CARD_ID, PAYMENT_ACCOUNT_ID ORDER BY EVENT_DATE_TIME ASC) AS TO_DATE
        ,TO_STATUS
        ,FROM_STATUS = 'ACTIVE' AS ACTIVE_LINK
    FROM
        pll_events
)

select * from from_to_dates
