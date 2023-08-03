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
         , LC328__SUCCESSFUL_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , 'JOINS' AS tab
    FROM {{ ref('lc__links_joins__monthly_retailer_channel') }}
    WHERE channel = 'LLOYDS'
      AND loyalty_plan_company NOT IN ('Loyalteas', 'Bink Sweet Shop'))

   , active AS (
    SELECT date
         , channel
         , brand
         , loyalty_plan_company
         , U109__ACTIVE_USERS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_UID
         , 'ACTIVE' AS tab
    FROM {{ ref('user__transactions__monthly_channel_brand_retailer') }}
    WHERE channel = 'LLOYDS'
      AND loyalty_plan_company NOT IN ('Loyalteas', 'Bink Sweet Shop'))

   , combine AS (
    SELECT date
         , tab
         , channel
         , brand
         , loyalty_plan_company
         , LC328__SUCCESSFUL_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , NULL AS U109__ACTIVE_USERS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_UID
    FROM joins
    UNION ALL
    SELECT date
         , tab
         , channel
         , brand
         , loyalty_plan_company
         , NULL AS LC328__SUCCESSFUL_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , U109__ACTIVE_USERS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_UID
    FROM active)

SELECT *
FROM combine
