/*
Test to ensure no create user events are followed by another create user event

Created By:     SP
Created Date:   2022/07/12
*/

{{ config(tags = ['business']) }}

WITH all_events AS (
    SELECT USER_ID,"EVENT_TYPE"
        ,LEAD("EVENT_TYPE") OVER
            (PARTITION BY USER_ID, EVENT_ID ORDER BY EVENT_DATE_TIME)
            AS NEXT_EVENT
    FROM {{ref('fact_user')}}
    WHERE "EVENT_TYPE" = 'CREATED'
)

,consecutive_creates AS (
    SELECT *
    FROM all_events
    WHERE NEXT_EVENT = 'CREATED'
)

SELECT *
FROM consecutive_creates
