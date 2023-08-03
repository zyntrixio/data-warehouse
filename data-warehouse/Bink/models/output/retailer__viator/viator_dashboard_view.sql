/*
Created by:         Christopher Mitchell
Created date:       2023-07-05
Last modified by:    
Last modified date: 

Description:
    Datasource to produce tableau dashboard for Viator - THIS IS A TEST
Parameters:
    source_object       - lc__links_joins__monthly_retailer
                        - trans__trans__monthly_retailer
                        - trans__avg__monthly_retailer
                        - user__transactions__monthly_retailer
*/

WITH lc_metric AS (
    SELECT *, 'JOINS' AS CATEGORY
    FROM {{ ref('lc__links_joins__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , txn_metrics AS (
    SELECT *, 'SPEND' AS CATEGORY
    FROM {{ ref('trans__trans__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , txn_avg AS (
    SELECT *, 'SPEND' AS CATEGORY
    FROM {{ ref('trans__avg__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , user_metrics AS (
    SELECT *, 'USERS' AS CATEGORY
    FROM {{ ref('user__transactions__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , pll_metrics AS (
     SELECT *, 'JOINS' AS CATEGORY
     FROM {{ ref('lc__pll__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator'
   ) 

    , combine_all AS (
    SELECT date
         , category 
         , loyalty_plan_name
         , loyalty_plan_company
         , lc347__successful_loyalty_card_joins__monthly_retailer__count
         , lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
    FROM lc_metric
    UNION ALL
    SELECT date
         , category
         , NULL AS loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , t011__txns__monthly_retailer__dcount
         , t009__spend__monthly_retailer__sum
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
    FROM txn_metrics
    UNION ALL
    SELECT date
         , category
         , NULL AS loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , t014__aov__monthly_retailer
         , t016__atf__monthly_retailer
         , t015__arpu__monthly_retailer
    FROM txn_avg
    UNION ALL
    SELECT date
         , category
         , NULL AS loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit
         , u107_active_users_brand_retailer_monthly__dcount_user
         , u108_active_users_brand_retailer_monthly__pit
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
    FROM user_metrics
    UNION ALL
    SELECT date
         , category
         , NULL AS loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , lc201__loyalty_card_active_pll__monthly_retailer__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
    FROM pll_metrics)

SELECT *
FROM combine_all