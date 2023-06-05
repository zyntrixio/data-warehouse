/*
Created by:         Aidan Summerville
Created date:       2022-04-22
Last modified by:   Christopher Mitchell
Last modified date: 05-06-2023

Description:
	DISABLED - dimension for active loyalty cards

Parameters:
    ref_object      - fact_loyalty_card
                    - fact_loyalty_card_removed

*/
{{ config(
  enabled= false
) }}

with loyalty_add as (
  select * 

  from {{ref('fact_loyalty_card')}} 

)


, loyalty_removed as (
select * 
  from {{ref('fact_loyalty_card_removed')}} 

)


   , events AS (
    SELECT a.event_id                                                                                                                                 AS add_id
         , a.event_date_time                                                                                                                          AS add_time
         , a.loyalty_card_id
         , a.user_id
         , a.channel
         , r.event_id                                                                                                                                 AS remove_id
         , r.event_date_time                                                                                                                          AS remove_time
         , ROW_NUMBER() OVER (PARTITION BY a.channel, a.user_id, a.loyalty_card_id ORDER BY DATEDIFF('second', a.event_date_time, r.event_date_time)) AS closest
         , DATEDIFF('second', a.event_date_time, r.event_date_time)
    FROM loyalty_add a
             LEFT JOIN loyalty_removed r
                       ON a.loyalty_card_id = r.loyalty_card_id
                           AND a.user_id = r.user_id
                           AND a.channel = r.channel
                           AND a.event_date_time <= r.event_date_time
    WHERE 1 = 1
      AND a.event_type = 'SUCCESS'
      AND DATE(a.event_date_time) >= '2022-06-09')

   , add_time AS (
    SELECT MIN(add_id)   AS add_id
         , MIN(add_time) AS add_time
         , loyalty_card_id
         , user_id
         , channel
         , remove_id
         , remove_time
         , CASE
               WHEN remove_id IS NOT NULL THEN 'TRUE'
               ELSE 'FALSE'
        END AS           removed
    FROM events
    WHERE (remove_id IS NULL OR (remove_id IS NOT NULL AND closest = 1))
          --and closest <> 1
    GROUP BY 3, 4, 5, 6, 7, 8)

SELECT loyalty_card_id
     , user_id
     , channel
     , removed
     , add_time                                                  AS valid_from
     , COALESCE(remove_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS valid_to
FROM add_time