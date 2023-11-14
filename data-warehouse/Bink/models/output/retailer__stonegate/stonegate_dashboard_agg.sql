/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-08
LAST MODIFIED BY:
LAST MODIFIED DATE:

DESCRIPTION:
    DATASOURCE TO PRODUCE TABLEAU DASHBOARD FOR STONEGATE GROUP - AGG VIEW
PARAMETERS:
    SOURCE_OBJECT       - LC__LINKS_JOINS__MONTHLY_RETAILER
                        - TRANS__TRANS__MONTHLY_RETAILER
                        - TRANS__AVG__MONTHLY_RETAILER
                        - USER__TRANSACTIONS__MONTHLY_RETAILER
                        - LC__PLL__MONTHLY_RETAILER
*/

WITH lc_metric AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__links_joins__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Stonegate Group'
),

txn_metrics AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__trans__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Stonegate Group'
),

txn_avg AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__avg__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Stonegate Group'
),

user_metrics AS (
    SELECT
        *,
        'USERS' AS category
    FROM {{ ref('user__transactions__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Stonegate Group'
),

pll_metrics AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__pll__monthly_retailer') }}
    WHERE loyalty_plan_company = 'Stonegate Group'
),

combine_all AS (
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        lc347__successful_loyalty_card_joins__monthly_retailer__count,
        lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t010__refund__monthly_retailer__sum,
        NULL AS t009__spend__monthly_retailer__sum,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg
    FROM lc_metric
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL
            AS lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        NULL
            AS lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        t011__txns__monthly_retailer__dcount,
        t012__refund__monthly_retailer__dcount,
        t010__refund__monthly_retailer__sum,
        t009__spend__monthly_retailer__sum,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg
    FROM txn_metrics
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL
            AS lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        NULL
            AS lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t010__refund__monthly_retailer__sum,
        NULL AS t009__spend__monthly_retailer__sum,
        t014__aov__monthly_retailer__avg,
        t015__arpu__monthly_retailer__avg,
        t016__atf__monthly_retailer__avg
    FROM txn_avg
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL
            AS lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        NULL
            AS lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        NULL AS lc201__loyalty_card_active_pll__monthly_retailer__pit,
        u107_active_users__retailer_monthly__dcount_uid,
        u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t010__refund__monthly_retailer__sum,
        NULL AS t009__spend__monthly_retailer__sum,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg
    FROM user_metrics
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL AS lc347__successful_loyalty_card_joins__monthly_retailer__count,
        NULL AS lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        NULL
            AS lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        NULL
            AS lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        lc201__loyalty_card_active_pll__monthly_retailer__pit,
        NULL AS u107_active_users__retailer_monthly__dcount_uid,
        NULL AS u108_active_users_retailer_monthly__cdcount_uid,
        NULL AS t011__txns__monthly_retailer__dcount,
        NULL AS t012__refund__monthly_retailer__dcount,
        NULL AS t010__refund__monthly_retailer__sum,
        NULL AS t009__spend__monthly_retailer__sum,
        NULL AS t014__aov__monthly_retailer__avg,
        NULL AS t015__arpu__monthly_retailer__avg,
        NULL AS t016__atf__monthly_retailer__avg
    FROM pll_metrics
)

SELECT *
FROM combine_all
