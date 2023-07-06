/*
Created by:         Christopher Mitchell
Created date:       2023-05-31
Last modified by:   Christopher Mitchell
Last modified date: 2023-06-06

Description:
    User table, which relates to the transform date into do date and from date for metrics layer

Parameters:
    ref_object      - src__fact_user
*/

WITH usr_events AS (
    SELECT *
    FROM {{ref('stg_metrics__fact_user')}})

, usr_stage AS (
    SELECT user_id
        , COALESCE(NULLIF(external_user_ref, ''), user_id) AS user_ref
        , event_id
        , event_type
        , channel
        , brand
        , event_date_time
    FROM usr_events)

, to_from_date AS (
    SELECT user_id
        , user_ref
        , event_id
        , event_type
        , channel
        , brand
        , event_date_time                                             AS from_date
        , LEAD(event_date_time)
            OVER (PARTITION BY user_ref ORDER BY event_date_time ASC) AS to_date
    FROM usr_stage)

, usr_final AS (
    SELECT event_id
        , user_ref
        , user_id
        , event_type
        , channel
        , brand
        , from_date
        , COALESCE(to_date, CURRENT_TIMESTAMP) AS to_date
    FROM to_from_date)

SELECT *
FROM usr_final
