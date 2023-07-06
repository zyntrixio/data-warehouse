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
    FROM {{ ref('src__fact_transaction') }})

   , metrics AS (
    SELECT DATE(DATE_TRUNC('month', date))                  AS date
         , channel
         , brand
         , loyalty_plan_company
         , COALESCE(NULLIF(external_user_ref, ''), user_id) AS u107__active_users_brand_retailer_monthly__dcount_user
    FROM user_events)

   , agg AS (
    SELECT date
         , channel
         , brand
         , loyalty_plan_company
         , COUNT(DISTINCT u107__active_users_brand_retailer_monthly__dcount_user) as u107__active_users_brand_retailer_monthly__dcount_user
    FROM metrics
    GROUP BY date, channel, brand, loyalty_plan_company)

SELECT *
FROM agg