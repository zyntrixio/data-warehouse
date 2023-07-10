/*
Created by:         Anand Bhakta
Created date:       2023-05-09
Last modified by:
Last modified date: 

Description:
    Set up of error to and from data for loyalty card error statuses excluding pending and active
Parameters:
    source_object       - src__fact_lc_status_change
                        - src__lookup_status_mapping
*/

WITH lc_sc AS (
    SELECT *
    FROM {{ref('stg_metrics__fact_lc_status_change')}}
)

,lc_lookup AS (
    SELECT *
    FROM {{ref('src__lookup_status_mapping')}}
)

,event_ordering AS ( -- Get Future And previous events per LC & User
    SELECT
        EVENT_DATE_TIME AS STATUS_START_TIME
        ,TO_STATUS_ID AS STATUS_ID
        ,TO_STATUS AS STATUS_DESCRIPTION
        ,CHANNEL
        ,LOYALTY_CARD_ID
        ,USER_ID
        ,EXTERNAL_USER_REF
        ,COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID) AS USER_REF
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,LEAD(EVENT_DATE_TIME, 1) OVER (PARTITION BY LOYALTY_PLAN_NAME, COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID) ORDER BY EVENT_DATE_TIME) AS STATUS_END_TIME
        ,LEAD(TO_STATUS_ID, 1) OVER (PARTITION BY LOYALTY_PLAN_NAME, COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID) ORDER BY EVENT_DATE_TIME) AS NEXT_STATUS_ID
        ,LAG(TO_STATUS_ID, 1 ) OVER (PARTITION BY LOYALTY_PLAN_NAME, COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID) ORDER BY EVENT_DATE_TIME) AS PREV_STATUS_ID
    FROM
        lc_sc
)

,join_status_types AS ( -- Join in lookup table to determine which status' are errors
    SELECT
        lc.*
        ,lcl.STATUS_TYPE
        ,lcl.STATUS_GROUP
        ,lcl.STATUS_ROLLUP
        ,lcl_next.STATUS_TYPE AS NEXT_STATUS_TYPE
    FROM event_ordering lc
    LEFT JOIN lc_lookup lcl
        ON lc.STATUS_ID = lcl.CODE
    LEFT JOIN lc_lookup lcl_prev
        ON lc.PREV_STATUS_ID = lcl_prev.CODE
    LEFT JOIN lc_lookup lcl_next
        ON lc.NEXT_STATUS_ID = lcl_next.CODE
    WHERE
        (lcl.STATUS_TYPE != 'Active' OR lcl_prev.STATUS_TYPE != 'Active') -- Ignore Active -> Active
        AND
        (lcl.STATUS_TYPE != 'Pending' OR lcl_prev.STATUS_TYPE != 'Pending') -- Ignore Pending -> Pending
)

,add_metrics AS ( -- Add useful reporting metrics & Calculate time differences between subsequent events
    SELECT
        *
        ,CASE WHEN
            PREV_STATUS_ID IS NOT NULL AND PREV_STATUS_ID = STATUS_ID
            THEN TRUE
            ELSE FALSE
            END AS REPEATED_STATUS
        ,CASE WHEN
            STATUS_TYPE = 'Error' AND NEXT_STATUS_TYPE IN ('Success')
            THEN TRUE
            ELSE FALSE
            END AS TO_RESOLVED
        ,CASE WHEN
            COALESCE(
                SUM(CASE WHEN STATUS_TYPE = 'Success' THEN 1 ELSE 0 END)
                OVER (PARTITION BY LOYALTY_PLAN_NAME, USER_REF ORDER BY STATUS_START_TIME
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING ), 0
                ) >= 1 THEN TRUE
            ELSE FALSE
            END AS is_resolved
        ,CASE WHEN STATUS_END_TIME IS NULL
            THEN TRUE
            ELSE FALSE
            END AS IS_FINAL_STATE
        ,DATEDIFF(day, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_DAYS
        ,DATEDIFF(hour, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_HOURS
        ,DATEDIFF(min, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_MINS
        ,DATEDIFF(sec, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_SECONDS
        ,DATEDIFF(millisecond, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_MILLISECONDS
    FROM join_status_types
)

,filter_non_error_events AS ( -- Filter out all non Error events
    SELECT
        STATUS_ID
        ,STATUS_DESCRIPTION
        ,STATUS_GROUP
        ,STATUS_ROLLUP
        ,STATUS_TYPE
        ,USER_ID
        ,EXTERNAL_USER_REF
        ,USER_REF
        ,CHANNEL
        ,BRAND
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,REPEATED_STATUS
        ,TO_RESOLVED
        ,IS_RESOLVED
        ,IS_FINAL_STATE
        ,STATUS_START_TIME
        ,STATUS_END_TIME
        ,TIMEDIFF_DAYS
        ,TIMEDIFF_HOURS
        ,TIMEDIFF_MINS
        ,TIMEDIFF_SECONDS
        ,TIMEDIFF_MILLISECONDS  
    FROM add_metrics lc
    -- WHERE STATUS_TYPE = 'Error'
)

SELECT *
FROM filter_non_error_events
