/*
Created by:         Christopher Mitchell
Created date:       2023-07-05
Last modified by:   Christopher Mitchell
Last modified date: 2024-01-15

Description:
    Datasource to produce tableau dashboard for Viator
Parameters:
    source_object       - lc__links_joins__monthly_retailer
                        - trans__trans__monthly_retailer
                        - trans__avg__monthly_retailer
                        - user__transactions__monthly_retailer
                        - voucher__counts__monthly_retailer
*/

WITH lc_metric AS (
    SELECT *,
           'JOINS' AS category
    FROM {{ ref('lc__links_joins__monthly_retailer__growth') }}
    WHERE loyalty_plan_company = 'Viator'),

     txn_metrics AS (
    SELECT *,
           'SPEND' AS category
    FROM {{ ref('trans__trans__monthly_retailer__growth') }}
    WHERE loyalty_plan_company = 'Viator'),

     txn_avg AS (
    SELECT *,
           'SPEND' AS category
    FROM {{ ref('trans__avg__monthly_retailer__growth') }}
    WHERE loyalty_plan_company = 'Viator'),

     user_metrics AS (
    SELECT *,
           'USERS' AS category
    FROM {{ ref('user__transactions__monthly_retailer__growth') }}
    WHERE loyalty_plan_company = 'Viator'),

     pll_metrics AS (
    SELECT *,
           'JOINS' AS category
    FROM {{ ref('lc__pll__monthly_retailer__growth') }}
    WHERE loyalty_plan_company = 'Viator'),

     voucher_metrics AS (
    SELECT *,
           'VOUCHERS' AS category
    FROM {{ ref('voucher__counts__monthly_retailer__growth') }}
    WHERE loyalty_plan_company = 'Viator'),

     combine_all AS (
    SELECT date,
           category,
           loyalty_plan_name,
           loyalty_plan_company,
           lc379__successful_loyalty_card_joins__monthly_retailer__csum__growth,
           lc347__successful_loyalty_card_joins__monthly_retailer__count__growth,
           lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count__growth,
           NULL AS t012__refund__monthly_retailer__dcount__growth,
           NULL AS t011__txns__monthly_retailer__dcount__growth,
           NULL AS t009__spend__monthly_retailer__sum__growth,
           NULL AS t014__aov__monthly_retailer__avg__growth,
           NULL AS t016__atf__monthly_retailer__avg__growth,
           NULL AS t015__arpu__monthly_retailer__avg__growth,
           NULL AS u107_active_users__retailer_monthly__dcount_uid__growth,
           NULL AS u108_active_users_retailer_monthly__cdcount_uid__growth,
           NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
           NULL AS v012__issued_vouchers__monthly_retailer__dcount__growth,
           NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher__growth,
           NULL AS v013__redeemed_vouchers__monthly_retailer__dcount__growth,
           NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher__growth
    FROM lc_metric
    UNION ALL
    SELECT date,
           category,
           loyalty_plan_name,
           loyalty_plan_company,
           NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum__growth,
           NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count__growth,
           NULL AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count__growth,
           t012__refund__monthly_retailer__dcount__growth,
           t011__txns__monthly_retailer__dcount__growth,
           t009__spend__monthly_retailer__sum__growth,
           NULL AS t014__aov__monthly_retailer__avg__growth,
           NULL AS t016__atf__monthly_retailer__avg__growth,
           NULL AS t015__arpu__monthly_retailer__avg__growth,
           NULL AS u107_active_users__retailer_monthly__dcount_uid__growth,
           NULL AS u108_active_users_retailer_monthly__cdcount_uid__growth,
           NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
           NULL AS v012__issued_vouchers__monthly_retailer__dcount__growth,
           NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher__growth,
           NULL AS v013__redeemed_vouchers__monthly_retailer__dcount__growth,
           NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher__growth
    FROM txn_metrics
    UNION ALL
    SELECT date,
           category,
           loyalty_plan_name,
           loyalty_plan_company,
           NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum__growth,
           NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count__growth,
           NULL AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count__growth,
           NULL AS t012__refund__monthly_retailer__dcount__growth,
           NULL AS t011__txns__monthly_retailer__dcount__growth,
           NULL AS t009__spend__monthly_retailer__sum__growth,
           t014__aov__monthly_retailer__avg__growth,
           t016__atf__monthly_retailer__avg__growth,
           t015__arpu__monthly_retailer__avg__growth,
           NULL AS u107_active_users__retailer_monthly__dcount_uid__growth,
           NULL AS u108_active_users_retailer_monthly__cdcount_uid__growth,
           NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
           NULL AS v012__issued_vouchers__monthly_retailer__dcount__growth,
           NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher__growth,
           NULL AS v013__redeemed_vouchers__monthly_retailer__dcount__growth,
           NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher__growth
    FROM txn_avg
    UNION ALL
    SELECT date,
           category,
           loyalty_plan_name,
           loyalty_plan_company,
           NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum__growth,
           NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count__growth,
           NULL AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count__growth,
           NULL AS t012__refund__monthly_retailer__dcount__growth,
           NULL AS t011__txns__monthly_retailer__dcount__growth,
           NULL AS t009__spend__monthly_retailer__sum__growth,
           NULL AS t014__aov__monthly_retailer__avg__growth,
           NULL AS t016__atf__monthly_retailer__avg__growth,
           NULL AS t015__arpu__monthly_retailer__avg__growth,
           u107_active_users__retailer_monthly__dcount_uid__growth,
           u108_active_users_retailer_monthly__cdcount_uid__growth,
           NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
           NULL AS v012__issued_vouchers__monthly_retailer__dcount__growth,
           NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher__growth,
           NULL AS v013__redeemed_vouchers__monthly_retailer__dcount__growth,
           NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher__growth
    FROM user_metrics
    UNION ALL
    SELECT date,
           category,
           loyalty_plan_name,
           loyalty_plan_company,
           NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum__growth,
           NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count__growth,
           NULL AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count__growth,
           NULL AS t012__refund__monthly_retailer__dcount__growth,
           NULL AS t011__txns__monthly_retailer__dcount__growth,
           NULL AS t009__spend__monthly_retailer__sum__growth,
           NULL AS t014__aov__monthly_retailer__avg__growth,
           NULL AS t016__atf__monthly_retailer__avg__growth,
           NULL AS t015__arpu__monthly_retailer__avg__growth,
           NULL AS u107_active_users__retailer_monthly__dcount_uid__growth,
           NULL AS u108_active_users_retailer_monthly__cdcount_uid__growth,
           lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
           NULL AS v012__issued_vouchers__monthly_retailer__dcount__growth,
           NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher__growth,
           NULL AS v013__redeemed_vouchers__monthly_retailer__dcount__growth,
           NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher__growth
    FROM pll_metrics
    UNION ALL
    SELECT date,
           category,
           loyalty_plan_name,
           loyalty_plan_company,
           NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum__growth,
           NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count__growth,
           NULL AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count__growth,
           NULL AS t012__refund__monthly_retailer__dcount__growth,
           NULL AS t011__txns__monthly_retailer__dcount__growth,
           NULL AS t009__spend__monthly_retailer__sum__growth,
           NULL AS t014__aov__monthly_retailer__avg__growth,
           NULL AS t016__atf__monthly_retailer__avg__growth,
           NULL AS t015__arpu__monthly_retailer__avg__growth,
           NULL AS u107_active_users__retailer_monthly__dcount_uid__growth,
           NULL AS u108_active_users_retailer_monthly__cdcount_uid__growth,
           NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
           v012__issued_vouchers__monthly_retailer__dcount__growth,
           v009__issued_vouchers__monthly_retailer__cdsum_voucher__growth,
           v013__redeemed_vouchers__monthly_retailer__dcount__growth,
           v010__redeemed_vouchers__monthly_retailer__cdsum_voucher__growth
    FROM voucher_metrics)

SELECT *
FROM combine_all
