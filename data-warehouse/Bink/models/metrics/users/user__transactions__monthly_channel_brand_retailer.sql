/*
Created by:         Christopher Mitchell 
Created date:       2023-07-03
Last modified by:   
Last modified date: 

Description:
    todo
Parameters:
    source_object       - src__fact_transaction
*/

WITH user_events AS (
    SELECT *
    FROM {{ ref('stg_metrics__fact_transaction') }})

   , metrics AS (
    SELECT DATE(DATE_TRUNC('month', date))                  AS date
         , channel
         , brand
         , loyalty_plan_company
         , loyalty_plan_name
         , COALESCE(NULLIF(external_user_ref, ''), user_id) AS u109__active_users__monthly_Channel_brand_retailer__dcount_uid
    FROM user_events)

   , agg AS (
    SELECT date
         , channel
         , brand
         , loyalty_plan_company
         , loyalty_plan_name
         , COUNT(DISTINCT u109__active_users__monthly_Channel_brand_retailer__dcount_uid) as U109__ACTIVE_USERS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_UID
    FROM metrics
    GROUP BY date, channel, brand, loyalty_plan_company)

SELECT *
FROM agg