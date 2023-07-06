/*
Created by:         Christopher Mitchell
Created date:       2023-06-07
Last modified by:    
Last modified date: 

Description:
    Rewrite of the LL table lc_joins_links_snapshot and lc_joins_links containing both snapshot and daily absolute data of all link and join journeys split by merchant.
Parameters:
    source_object       - src__fact_lc_add
                        - src__fact_lc_removed
                        - src__dim_loyalty_card
                        - src__dim_date
*/

WITH lc_events AS (
    SELECT *
    FROM {{ref('lc_trans')}}
)

,dim_date AS (
    SELECT *
    FROM {{ref('stg_metrics__dim_date')}}
    WHERE
        DATE >= (SELECT MIN(FROM_DATE) FROM lc_events)
        AND DATE <= CURRENT_DATE()
)
        
,count_up_snap AS (
  SELECT
    d.DATE
    ,u.CHANNEL
    ,u.BRAND
        -- Links and Joins
        ,COALESCE(COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'SUCCESS' THEN USER_REF END),0)              AS U003__USERS_WITH_A_LINKED_LOYALTY_CARD__DAILY_CHANNEL_BRAND__PIT
FROM lc_events u
LEFT JOIN dim_date d
    ON d.DATE >= DATE(u.FROM_DATE)
    AND d.DATE < COALESCE(DATE(u.TO_DATE), '9999-12-31')
GROUP BY
    d.DATE
    ,u.BRAND
    ,u.CHANNEL
HAVING DATE IS NOT NULL
)   

SELECT * 
FROM count_up_snap
