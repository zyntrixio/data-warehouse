/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-08
LAST MODIFIED BY:
LAST MODIFIED DATE:

DESCRIPTION:
    DATASOURCE TO PRODUCE TABLEAU DASHBOARD FOR STONEGATE GROUP - BINK VIEW
PARAMETERS:
    SOURCE_OBJECT       - LC__LINKS_JOINS__monthly_channel_brand_retailer
                        - TRANS__TRANS__monthly_channel_brand_retailer
                        - TRANS__AVG__monthly_channel_retailer
                        - USER__TRANSACTIONS__monthly_channel_brand_retailer
                        - LC__PLL__monthly_channel_brand_retailer
*/

WITH lc_metric AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__links_joins__monthly_channel_brand_retailer') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

txn_metrics AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__trans__monthly_channel_brand_retailer') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

txn_avg AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__avg__monthly_channel_retailer') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

user_metrics AS (
    SELECT
        *,
        'USERS' AS category
    FROM {{ ref('user__transactions__monthly_channel_brand_retailer') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

pll_metrics AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__pll__monthly_channel_brand_retailer') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

combine_all AS (
    SELECT
        date,
        category,
        channel,
        brand,
        loyalty_plan_name,
        loyalty_plan_company,
        lc320__successful_loyalty_card_joins__monthly_channel_brand_retailer__count,
        lc324__successful_loyalty_card_links__monthly_channel_brand_retailer__dcount_user,
        lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum,
        lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit
    FROM lc_metric
    UNION ALL
    SELECT
        date,
        category,
        channel,
        brand,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS lc320__successful_loyalty_card_joins__monthly_channel_brand_retailer__count,
        NULL
            AS lc324__successful_loyalty_card_links__monthly_channel_brand_retailer__dcount_user,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage,
        t049__spend__monthly_channel_brand_retailer__sum,
        t051__txns__monthly_channel_brand_retailer__dcount,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit
    FROM txn_metrics
    UNION ALL
    SELECT
        date,
        category,
        channel,
        NULL AS brand,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS lc320__successful_loyalty_card_joins__monthly_channel_brand_retailer__count,
        NULL
            AS lc324__successful_loyalty_card_links__monthly_channel_brand_retailer__dcount_user,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount,
        t054__aov__monthly_channel_brand_retailer__avg,
        t055__arpu__monthly_channel_brand_retailer__avg,
        t056__atf__monthly_channel_brand_retailer__avg,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit
    FROM txn_avg
    UNION ALL
    SELECT
        date,
        category,
        channel,
        brand,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS lc320__successful_loyalty_card_joins__monthly_channel_brand_retailer__count,
        NULL
            AS lc324__successful_loyalty_card_links__monthly_channel_brand_retailer__dcount_user,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg,
        u200_active_users__monthly_channel_brand_retailer__dcount_uid,
        u201_active_users_monthly_channel_brand_retailer__cdcount_uid,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit
    FROM user_metrics
    UNION ALL
    SELECT
        date,
        category,
        channel,
        brand,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS lc320__successful_loyalty_card_joins__monthly_channel_brand_retailer__count,
        NULL
            AS lc324__successful_loyalty_card_links__monthly_channel_brand_retailer__dcount_user,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid,
        lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit
    FROM pll_metrics
)

SELECT *
FROM combine_all
