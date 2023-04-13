/*
Test to ensure no create user events are followed by another create user event

Created By:     SP
Created Date:   2022/07/12
*/

{{ config(
        tags=['business']
        ,meta={"description": "est to ensure no create user events are followed by another create user event in last 24 hours.", 
            "test_type": "Business"},
) }}

WITH all_events AS (
    SELECT USER_ID,"EVENT_TYPE"
        ,LEAD("EVENT_TYPE") OVER
            (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME, EVENT_ID)
            AS NEXT_EVENT
    FROM {{ref('fact_user')}}
    WHERE "EVENT_TYPE" = 'CREATED'
    AND TIMEDIFF(
                hour, EVENT_DATE_TIME, (
                    SELECT max(EVENT_DATE_TIME)
                    FROM {{ref('fact_user')}}
                    )
                ) < 24
)

,consecutive_creates AS (
    SELECT *
    FROM all_events
    WHERE NEXT_EVENT = 'CREATED'
)

SELECT *
FROM consecutive_creates
