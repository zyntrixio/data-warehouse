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
    SELECT *, 'LC_METRIC' AS tab
    FROM {{ ref('lc__links_joins__monthly_retailer') }})

   , txn_metrics AS (
    SELECT *, 'TXN METRIC' AS tab
    FROM {{ ref('trans__trans__monthly_retailer') }})

   , txn_avg AS (
    SELECT *, 'TXN_AVG' AS tab
    FROM {{ ref('trans__avg__monthly_retailer') }})

   , user_metrics AS (
    SELECT *, 'USER_METRIC' AS tab
    FROM {{ ref('user__transactions__monthly_retailer') }})

   , combine_all AS (
    SELECT date
         , tab
         , loyalty_plan_name
         , loyalty_plan_company
         , lc339__successful_loyalty_cards__monthly_retailer__count
         , lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t010__refund__monthly_retailer__sum
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t012__refund__monthly_retailer__dcount
         , NULL AS t013__bnpl_txns__monthly_retailer__dcount
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM lc_metric
    UNION ALL
    SELECT date
         , tab
         , loyalty_plan_company
         , NULL AS loyalty_plan_name
         , NULL AS lc339__successful_loyalty_cards__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , t009__spend__monthly_retailer__sum
         , t010__refund__monthly_retailer__sum
         , t011__txns__monthly_retailer__dcount
         , t012__refund__monthly_retailer__dcount
         , t013__bnpl_txns__monthly_retailer__dcount
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM txn_metrics
    UNION ALL
    SELECT date
         , tab
         , loyalty_plan_company
         , NULL AS loyalty_plan_name
         , NULL AS lc339__successful_loyalty_cards__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t010__refund__monthly_retailer__sum
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t012__refund__monthly_retailer__dcount
         , NULL AS t013__bnpl_txns__monthly_retailer__dcount
         , t014__aov__monthly_retailer
         , t015__arpu__monthly_retailer
         , t016__atf__monthly_retailer
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM txn_avg
    UNION ALL
    SELECT date
         , tab
         , loyalty_plan_company
         , NULL AS loyalty_plan_name
         , NULL AS lc339__successful_loyalty_cards__monthly_retailer__count
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t010__refund__monthly_retailer__sum
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t012__refund__monthly_retailer__dcount
         , NULL AS t013__bnpl_txns__monthly_retailer__dcount
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , u108_active_users_brand_retailer_monthly__pit
         , u107_active_users_brand_retailer_monthly__dcount_user
    FROM user_metrics)

SELECT *
FROM combine_all
WHERE loyalty_plan_name IN ('Together Rewards', 'The Works')
   OR loyalty_plan_company IN ('Together Rewards', 'The Works')