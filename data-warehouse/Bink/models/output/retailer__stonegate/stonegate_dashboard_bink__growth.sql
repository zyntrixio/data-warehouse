/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-15
LAST MODIFIED BY:
LAST MODIFIED DATE:

DESCRIPTION:
    DATASOURCE TO PRODUCE TABLEAU DASHBOARD FOR STONEGATE GROUP - BINK VIEW
PARAMETERS:
    SOURCE_OBJECT       - LC__LINKS_JOINS__monthly_channel_brand_retailer__GROWTH
                        - TRANS__TRANS__monthly_channel_brand_retailer__GROWTH
                        - TRANS__AVG__monthly_channel_brand_retailer__GROWTH
                        - USER__TRANSACTIONS__monthly_channel_brand_retailer__GROWTH
                        - LC__PLL__monthly_channel_brand_retailer__GROWTH
*/

WITH lc_metric AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__links_joins__monthly_channel_retailer__growth') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

txn_metrics AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__trans__monthly_channel_retailer__growth') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

txn_avg AS (
    SELECT
        *,
        'SPEND' AS category
    FROM {{ ref('trans__avg__monthly_channel_retailer__growth') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

user_metrics AS (
    SELECT
        *,
        'USERS' AS category
    FROM {{ ref('user__transactions__monthly_channel_retailer__growth') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

pll_metrics AS (
    SELECT
        *,
        'JOINS' AS category
    FROM {{ ref('lc__pll__monthly_channel_retailer__growth') }}
    WHERE
        loyalty_plan_company = 'Stonegate Group'
        AND channel = 'LLOYDS'
),

combine_all AS (
    SELECT
        date,
        category,
        channel,
        loyalty_plan_name,
        loyalty_plan_company,
        LC312__SUCCESSFUL_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT__GROWTH,
        LC324__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER__GROWTH,
        lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum__growth,
        lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage__growth,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum__growth,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount__growth,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg__growth,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg__growth,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg__growth,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid__growth,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid__growth,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit__growth
    FROM lc_metric
    UNION ALL
    SELECT
        date,
        category,
        channel,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS LC312__SUCCESSFUL_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT__GROWTH,
        NULL
            AS LC324__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER__GROWTH,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum__growth,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage__growth,
        t049__spend__monthly_channel_brand_retailer__sum__growth,
        t051__txns__monthly_channel_brand_retailer__dcount__growth,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg__growth,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg__growth,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg__growth,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid__growth,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid__growth,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit__growth
    FROM txn_metrics
    UNION ALL
    SELECT
        date,
        category,
        channel,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS LC312__SUCCESSFUL_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT__GROWTH,
        NULL
            AS LC324__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER__GROWTH,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum__growth,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage__growth,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum__growth,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount__growth,
        t054__aov__monthly_channel_brand_retailer__avg__growth,
        t055__arpu__monthly_channel_brand_retailer__avg__growth,
        t056__atf__monthly_channel_brand_retailer__avg__growth,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid__growth,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid__growth,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit__growth
    FROM txn_avg
    UNION ALL
    SELECT
        date,
        category,
        channel,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS LC312__SUCCESSFUL_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT__GROWTH,
        NULL
            AS LC324__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER__GROWTH,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum__growth,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage__growth,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum__growth,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount__growth,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg__growth,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg__growth,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg__growth,
        u200_active_users__monthly_channel_brand_retailer__dcount_uid__growth,
        u201_active_users_monthly_channel_brand_retailer__cdcount_uid__growth,
        NULL AS lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit__growth
    FROM user_metrics
    UNION ALL
    SELECT
        date,
        category,
        channel,
        loyalty_plan_name,
        loyalty_plan_company,
        NULL
            AS LC312__SUCCESSFUL_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT__GROWTH,
        NULL
            AS LC324__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER__GROWTH,
        NULL
            AS lc371__successful_loyalty_card_joins__monthly_channel_brand_retailer__csum__growth,
        NULL
            AS lc385__successful_loyalty_card_join_mrkt_opt_in_percentage__monthly_channel_brand_retailer__percentage__growth,
        NULL AS t049__spend__monthly_channel_brand_retailer__sum__growth,
        NULL AS t051__txns__monthly_channel_brand_retailer__dcount__growth,
        NULL AS t054__aov__monthly_channel_brand_retailer__avg__growth,
        NULL AS t055__arpu__monthly_channel_brand_retailer__avg__growth,
        NULL AS t056__atf__monthly_channel_brand_retailer__avg__growth,
        NULL AS u200_active_users__monthly_channel_brand_retailer__dcount_uid__growth,
        NULL AS u201_active_users_monthly_channel_brand_retailer__cdcount_uid__growth,
        lc386__loyalty_card_active_pll__monthly_channel_brand_retailer__pit__growth
    FROM pll_metrics
)

SELECT *
FROM combine_all
