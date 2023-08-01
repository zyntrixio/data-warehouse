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
    SELECT *, 'LC_METRIC' AS tab
    FROM {{ ref('lc__links_joins__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , txn_metrics AS (
    SELECT *, 'TXN METRIC' AS tab
    FROM {{ ref('trans__trans__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , txn_avg AS (
    SELECT *, 'TXN_AVG' AS tab
    FROM {{ ref('trans__avg__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , user_metrics AS (
    SELECT *, 'USER_METRIC' AS tab
    FROM {{ ref('user__transactions__monthly_retailer') }} WHERE loyalty_plan_company = 'Viator')

   , combine_all AS (
    SELECT date
         , tab
         , loyalty_plan_company
         , loyalty_plan_name
         , lc335__successful_loyalty_cards__monthly_channel_brand_retailer__pit
         , lc336__requests_loyalty_cards__monthly_channel_brand_retailer__pit
         , lc337__failed_loyalty_cards__monthly_channel_brand_retailer__pit
         , lc338__deleted_loyalty_cards__monthly_channel_brand_retailer__pit
         , lc359__successful_loyalty_card_links__monthly_retailer__pit
         , lc360__requests_loyalty_card_links__monthly_retailer__pit
         , lc361__failed_loyalty_card_links__monthly_retailer__pit
         , lc362__deleted_loyalty_card_links__monthly_retailer__pit
         , lc363__successful_loyalty_card_joins__monthly_retailer__pit
         , lc364__requests_loyalty_card_joins__monthly_retailer__pit
         , lc365__failed_loyalty_card_joins__monthly_retailer__pit
         , lc366__deleted_loyalty_card_joins__monthly_retailer__pit
         , lc339__successful_loyalty_cards__monthly_retailer__count
         , lc340__requests_loyalty_cards__monthly_retailer__count
         , lc341__failed_loyalty_cards__monthly_retailer__count
         , lc342__deleted_loyalty_cards__monthly_retailer__count
         , lc343__successful_loyalty_card_links__monthly_retailer__count
         , lc344__requests_loyalty_card_links__monthly_retailer__count
         , lc345__failed_loyalty_card_links__monthly_retailer__count
         , lc346__deleted_loyalty_card_links__monthly_retailer__count
         , lc347__successful_loyalty_card_joins__monthly_retailer__count
         , lc348__successful_loyalty_card_joins__monthly_retailer__count
         , lc349__requests_loyalty_card_joins__monthly_retailer__count
         , lc350__failed_loyalty_card_joins__monthly_retailer__count
         , lc375__successful_loyalty_card_links__monthly_retailer__csum
         , lc376__requests_loyalty_card_links__monthly_retailer__csum
         , lc377__failed_loyalty_card_links__monthly_retailer__csum
         , lc378__deleted_loyalty_card_links__monthly_retailer__csum
         , lc379__successful_loyalty_card_joins__monthly_retailer__csum
         , lc380__requests_loyalty_card_joins__monthly_retailer__csum
         , lc381__failed_loyalty_card_joins__monthly_retailer__csum
         , lc382__deleted_loyalty_card_joins__monthly_retailer__csum
         , lc351__successful_loyalty_card_links__monthly_retailer__dcount_user
         , lc352__requests_loyalty_card_links__monthly_retailer__dcount_user
         , lc353__failed_loyalty_card_links__monthly_retailer__dcount_user
         , lc354__deleted_loyalty_card_links__monthly_retailer__dcount_user
         , lc355__successful_loyalty_card_joins__monthly_retailer__dcount_user
         , lc356__requests_loyalty_card_joins__monthly_retailer__dcount_user
         , lc357__failed_loyalty_card_joins__monthly_retailer__dcount_user
         , lc358__deleted_loyalty_card_joins__monthly_retailer__dcount_user
         , lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , lc332__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__csum
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t010__refund__monthly_retailer__sum
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t012__refund__monthly_retailer__dcount
         , NULL AS t013__bnpl_txns__monthly_retailer__dcount
         , NULL AS t004__spend__monthly_retailer__pit
         , NULL AS t005__refund__monthly_retailer__pit
         , NULL AS t006__txns__monthly_retailer__pit
         , NULL AS t007__refund__monthly_retailer__pit
         , NULL AS t008__bnpl_txns__monthly_retailer__pit
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM lc_metric
    UNION ALL
    SELECT date
         , tab
         , NULL AS loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc335__successful_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc336__requests_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc337__failed_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc338__deleted_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc359__successful_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc360__requests_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc361__failed_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc362__deleted_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc363__successful_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc364__requests_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc365__failed_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc366__deleted_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc339__successful_loyalty_cards__monthly_retailer__count
         , NULL AS lc340__requests_loyalty_cards__monthly_retailer__count
         , NULL AS lc341__failed_loyalty_cards__monthly_retailer__count
         , NULL AS lc342__deleted_loyalty_cards__monthly_retailer__count
         , NULL AS lc343__successful_loyalty_card_links__monthly_retailer__count
         , NULL AS lc344__requests_loyalty_card_links__monthly_retailer__count
         , NULL AS lc345__failed_loyalty_card_links__monthly_retailer__count
         , NULL AS lc346__deleted_loyalty_card_links__monthly_retailer__count
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc348__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc349__requests_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc350__failed_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc375__successful_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc376__requests_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc377__failed_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc378__deleted_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc380__requests_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc381__failed_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc382__deleted_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc351__successful_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc352__requests_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc353__failed_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc354__deleted_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc355__successful_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc356__requests_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc357__failed_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc358__deleted_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc332__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__csum
         , t009__spend__monthly_retailer__sum
         , t010__refund__monthly_retailer__sum
         , t011__txns__monthly_retailer__dcount
         , t012__refund__monthly_retailer__dcount
         , t013__bnpl_txns__monthly_retailer__dcount
         , t004__spend__monthly_retailer__pit
         , t005__refund__monthly_retailer__pit
         , t006__txns__monthly_retailer__pit
         , t007__refund__monthly_retailer__pit
         , t008__bnpl_txns__monthly_retailer__pit
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM txn_metrics
    UNION ALL
    SELECT date
         , tab
         , NULL AS loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc335__successful_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc336__requests_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc337__failed_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc338__deleted_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc359__successful_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc360__requests_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc361__failed_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc362__deleted_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc363__successful_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc364__requests_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc365__failed_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc366__deleted_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc339__successful_loyalty_cards__monthly_retailer__count
         , NULL AS lc340__requests_loyalty_cards__monthly_retailer__count
         , NULL AS lc341__failed_loyalty_cards__monthly_retailer__count
         , NULL AS lc342__deleted_loyalty_cards__monthly_retailer__count
         , NULL AS lc343__successful_loyalty_card_links__monthly_retailer__count
         , NULL AS lc344__requests_loyalty_card_links__monthly_retailer__count
         , NULL AS lc345__failed_loyalty_card_links__monthly_retailer__count
         , NULL AS lc346__deleted_loyalty_card_links__monthly_retailer__count
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc348__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc349__requests_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc350__failed_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc375__successful_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc376__requests_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc377__failed_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc378__deleted_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc380__requests_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc381__failed_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc382__deleted_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc351__successful_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc352__requests_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc353__failed_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc354__deleted_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc355__successful_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc356__requests_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc357__failed_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc358__deleted_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc332__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__csum
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t010__refund__monthly_retailer__sum
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t012__refund__monthly_retailer__dcount
         , NULL AS t013__bnpl_txns__monthly_retailer__dcount
         , NULL AS t004__spend__monthly_retailer__pit
         , NULL AS t005__refund__monthly_retailer__pit
         , NULL AS t006__txns__monthly_retailer__pit
         , NULL AS t007__refund__monthly_retailer__pit
         , NULL AS t008__bnpl_txns__monthly_retailer__pit
         , t014__aov__monthly_retailer
         , t015__arpu__monthly_retailer
         , t016__atf__monthly_retailer
         , NULL AS u108_active_users_brand_retailer_monthly__pit
         , NULL AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM txn_avg
    UNION ALL
    SELECT date
         , tab
         , NULL AS loyalty_plan_name
         , loyalty_plan_company
         , NULL AS lc335__successful_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc336__requests_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc337__failed_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc338__deleted_loyalty_cards__monthly_channel_brand_retailer__pit
         , NULL AS lc359__successful_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc360__requests_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc361__failed_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc362__deleted_loyalty_card_links__monthly_retailer__pit
         , NULL AS lc363__successful_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc364__requests_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc365__failed_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc366__deleted_loyalty_card_joins__monthly_retailer__pit
         , NULL AS lc339__successful_loyalty_cards__monthly_retailer__count
         , NULL AS lc340__requests_loyalty_cards__monthly_retailer__count
         , NULL AS lc341__failed_loyalty_cards__monthly_retailer__count
         , NULL AS lc342__deleted_loyalty_cards__monthly_retailer__count
         , NULL AS lc343__successful_loyalty_card_links__monthly_retailer__count
         , NULL AS lc344__requests_loyalty_card_links__monthly_retailer__count
         , NULL AS lc345__failed_loyalty_card_links__monthly_retailer__count
         , NULL AS lc346__deleted_loyalty_card_links__monthly_retailer__count
         , NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc348__successful_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc349__requests_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc350__failed_loyalty_card_joins__monthly_retailer__count
         , NULL AS lc375__successful_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc376__requests_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc377__failed_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc378__deleted_loyalty_card_links__monthly_retailer__csum
         , NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc380__requests_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc381__failed_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc382__deleted_loyalty_card_joins__monthly_retailer__csum
         , NULL AS lc351__successful_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc352__requests_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc353__failed_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc354__deleted_loyalty_card_links__monthly_retailer__dcount_user
         , NULL AS lc355__successful_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc356__requests_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc357__failed_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc358__deleted_loyalty_card_joins__monthly_retailer__dcount_user
         , NULL AS lc333__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , NULL AS lc332__sucessful_loyalty_card_join_mrkt_opt_in__monthly_retailer__csum
         , NULL AS t009__spend__monthly_retailer__sum
         , NULL AS t010__refund__monthly_retailer__sum
         , NULL AS t011__txns__monthly_retailer__dcount
         , NULL AS t012__refund__monthly_retailer__dcount
         , NULL AS t013__bnpl_txns__monthly_retailer__dcount
         , NULL AS t004__spend__monthly_retailer__pit
         , NULL AS t005__refund__monthly_retailer__pit
         , NULL AS t006__txns__monthly_retailer__pit
         , NULL AS t007__refund__monthly_retailer__pit
         , NULL AS t008__bnpl_txns__monthly_retailer__pit
         , NULL AS t014__aov__monthly_retailer
         , NULL AS t015__arpu__monthly_retailer
         , NULL AS t016__atf__monthly_retailer
         , u108_active_users_brand_retailer_monthly__pit
         , u107_active_users_brand_retailer_monthly__dcount_user
    FROM user_metrics)

SELECT *
FROM combine_all