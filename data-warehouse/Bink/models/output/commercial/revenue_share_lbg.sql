/*
Created by:          Christopher Mitchell
Created date:        2023-07-04
Last modified by:   
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - users_overview
Parameters:
    source_object       - lc__links_joins__monthly_retailer_channel
                        - User__transactions__monthly_user_level
*/

WITH joins AS (
    SELECT date
         , channel
         , brand
         , loyalty_plan_name
         , loyalty_plan_company
         , lc053__successful_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
         , 'JOINS' AS tab
    FROM {{ ref('lc__links_joins__monthly_retailer_channel') }}
    WHERE channel = 'LLOYDS'
      AND loyalty_plan_company NOT IN ('Loyalteas', 'Bink Sweet Shop'))

   , active AS (
    SELECT date
         , channel
         , brand
         , loyalty_plan_company
         , u107__active_users_brand_retailer_monthly__dcount_user
         , 'ACTIVE' AS tab
    FROM {{ ref('user__transactions__monthly_user_level') }}
    WHERE channel = 'LLOYDS'
      AND loyalty_plan_company NOT IN ('Loyalteas', 'Bink Sweet Shop'))

   , combine AS (
    SELECT date
         , tab
         , channel
         , brand
         , loyalty_plan_name
         , loyalty_plan_company
         , lc053__successful_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
         , NULL AS u107__active_users_brand_retailer_monthly__dcount_user
    FROM joins
    UNION ALL
    SELECT date
         , tab
         , channel
         , brand
         , loyalty_plan_company
         , NULL AS loyalty_plan_name
         , NULL AS lc053__successful_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
         , u107__active_users_brand_retailer_monthly__dcount_user
    FROM active)

SELECT *
FROM combine