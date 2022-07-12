/*
Test to ensure all delete users events have a corresponding create event

Created By:     SP
Created Date:   2022/07/12
*/

{{ config(tags = ['business']) }}

WITH event_vals AS (
    SELECT 
        CASE WHEN event_type = 'CREATED'
            THEN 1
            ELSE -1
            END AS event_val
        ,external_user_ref
        ,event_date_time
    FROM {{ref('fact_user_secure')}}
    WHERE external_user_ref IS NOT null
)

,sum_event_vals AS (
    SELECT
        external_user_ref,
        SUM(event_val) s,
        MAX(event_date_time) max_time
    FROM
        event_vals
    GROUP BY
        external_user_ref
    HAVING
        s < 0
        AND TIMEDIFF(hour, max_time, sysdate()) < 24
)


SELECT *
FROM sum_event_vals