/*
Created by:         Christopher Mitchell
Created date:       2023-07-05
Last modified by:    
Last modified date: 

Description:
    Datasource to produce tableau dashboard for The Works
Parameters:
    source_object       - lc__links_joins__monthly_retailer
                        - trans__trans__monthly_retailer
                        - trans__avg__monthly_retailer
                        - user__transactions__monthly_retailer
*/

WITH lc_metric AS (
    SELECT *, 'JOINS' AS CATEGORY
    FROM {{ ref('lc__links_joins__monthly_retailer') }} WHERE loyalty_plan_company = 'The Works')

   , txn_metrics AS (
    SELECT *, 'SPEND' AS CATEGORY
    FROM {{ ref('trans__trans__monthly_retailer') }} WHERE loyalty_plan_company = 'The Works')

   , txn_avg AS (
    SELECT *, 'SPEND' AS CATEGORY
    FROM {{ ref('trans__avg__monthly_retailer') }} WHERE loyalty_plan_company = 'The Works')

   , user_metrics AS (
    SELECT *, 'USERS' AS CATEGORY
    FROM {{ ref('user__transactions__monthly_retailer') }} WHERE loyalty_plan_company = 'The Works')

   , pll_metrics AS (
     SELECT *, 'JOINS' AS CATEGORY
     FROM {{ ref('lc__pll__monthly_retailer') }} WHERE loyalty_plan_company = 'The Works'
   ) 

   , combine_all AS (
    SELECT date
         , category
         , loyalty_plan_name
         , loyalty_plan_company
         , lc347__successful_loyalty_card_joins__monthly_retailer__count
         , lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID
         , NULL AS U108_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__CDCOUNT_UID
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS T014__AOV__MONTHLY_RETAILER__AVG__AVG
         , NULL AS T016__ATF__MONTHLY_RETAILER__AVG
         , NULL AS T015__ARPU__MONTHLY_RETAILER__AVG
    FROM lc_metric
    UNION ALL
    SELECT date
         , category
         , loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID
         , NULL AS U108_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__CDCOUNT_UID
         , t011__txns__monthly_retailer__dcount
         , t009__spend__monthly_retailer__sum
         , NULL AS T014__AOV__MONTHLY_RETAILER__AVG
         , NULL AS T016__ATF__MONTHLY_RETAILER__AVG
         , NULL AS T015__ARPU__MONTHLY_RETAILER__AVG
    FROM txn_metrics
    UNION ALL
    SELECT date
         , category
         , loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID
         , NULL AS U108_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__CDCOUNT_UID
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , T014__AOV__MONTHLY_RETAILER__AVG
         , T016__ATF__MONTHLY_RETAILER__AVG
         , T015__ARPU__MONTHLY_RETAILER__AVG
    FROM txn_avg
    UNION ALL
    SELECT date
         , category
         , loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID
         , U108_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__CDCOUNT_UID
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS T014__AOV__MONTHLY_RETAILER__AVG
         , NULL AS T016__ATF__MONTHLY_RETAILER__AVG
         , NULL AS T015__ARPU__MONTHLY_RETAILER__AVG
    FROM user_metrics
    UNION ALL
    SELECT date
         , category
         , loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID
         , NULL AS U108_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__CDCOUNT_UID
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS T014__AOV__MONTHLY_RETAILER__AVG
         , NULL AS T016__ATF__MONTHLY_RETAILER__AVG
         , NULL AS T015__ARPU__MONTHLY_RETAILER__AVG
    FROM pll_metrics)

SELECT *
FROM combine_all