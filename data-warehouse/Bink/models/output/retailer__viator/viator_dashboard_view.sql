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
*/

WITH lc_metric AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__links_joins__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Viator'
),

txn_metrics AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__trans__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Viator'
),

txn_avg AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__avg__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Viator'
),

user_metrics AS (
    SELECT
        *,
        'USERS' AS category
    FROM {{ ref('user__transactions__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Viator'
),

pll_metrics AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__pll__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Viator'
),

voucher_metrics AS (
    SELECT
        *,
        'VOUCHERS' AS category
    FROM {{ ref('voucher__counts__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Viator'
),

combine_all AS (
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        lc347__successful_loyalty_card_joins__monthly_retailer__count,
        lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS t009__spend__monthly_retailer__sum,
        NULL AS v012__issued_vouchers__monthly_retailer__dcount,
        NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        NULL AS v013__redeemed_vouchers__monthly_retailer__dcount,
        NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    FROM lc_metric
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL
            AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        t012__refund__monthly_retailer__dcount,
        t011__txns__monthly_retailer__dcount,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        t009__spend__monthly_retailer__sum,
        NULL AS v012__issued_vouchers__monthly_retailer__dcount,
        NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        NULL AS v013__redeemed_vouchers__monthly_retailer__dcount,
        NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    FROM txn_metrics
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL
            AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        t014__aov__monthly_retailer__avg,
        t016__atf__monthly_retailer__avg,
        t015__arpu__monthly_retailer__avg,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS t009__spend__monthly_retailer__sum,
        NULL AS v012__issued_vouchers__monthly_retailer__dcount,
        NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        NULL AS v013__redeemed_vouchers__monthly_retailer__dcount,
        NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    FROM txn_avg
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL
            AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t011__txns__monthly_retailer__dcount,
        u107_active_users__retailer_monthly__dcount_uid,
        u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS t009__spend__monthly_retailer__sum,
        NULL AS v012__issued_vouchers__monthly_retailer__dcount,
        NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        NULL AS v013__redeemed_vouchers__monthly_retailer__dcount,
        NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    FROM user_metrics
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL
            AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS t009__spend__monthly_retailer__sum,
        NULL AS v012__issued_vouchers__monthly_retailer__dcount,
        NULL AS v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        NULL AS v013__redeemed_vouchers__monthly_retailer__dcount,
        NULL AS v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    FROM pll_metrics
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL
            AS lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS t009__spend__monthly_retailer__sum,
        v012__issued_vouchers__monthly_retailer__dcount,
        v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        v013__redeemed_vouchers__monthly_retailer__dcount,
        v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    FROM voucher_metrics
)

SELECT *
FROM combine_all
