/*
Created by:         Christopher Mitchell
Created date:       2023-05-
Last modified by:   
Last modified date: 

Description:
    User table, which relates to the transform date into do date and from date for metrics layer

Parameters:
    ref_object      - src__fact_user
					- dim_user?
*/

WITH usr_events AS (SELECT event_id
                         , event_date_time
                         , user_id
                         , event_type
                         , is_most_recent
                         , origin
                         , channel
                         , brand
                         , inserted_date_time
                         , updated_date_time
                    FROM {{ref(src__fact_user)}})
   , to_from_date AS (SELECT user_id
                           , event_id
                           , event_type
                           , origin
                           , channel
                           , brand
                           , event_date_time
                           , LAG(event_date_time) OVER (PARTITION BY user_id ORDER BY event_date_time)  AS from_date
                           , LEAD(event_date_time) OVER (PARTITION BY user_id ORDER BY event_date_time) AS to_date
                      FROM usr_events)
   , usr_stage AS (SELECT event_id
                        , event_date_time
                        , user_id
                        , event_type
                        , origin
                        , channel
                        , brand
                        , COALESCE(from_date, event_date_time) AS from_date
                        , COALESCE(to_date, event_date_time)   AS to_date
                   FROM to_from_date)
SELECT *
FROM usr_stage