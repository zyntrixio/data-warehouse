/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-12
Last modified by: Anand Bhakta
Last modified date: 2023-12-19

DESCRIPTION:
    OUTPUT LAYER FOR EXT DASHBOARD
PARAMETERS:
    SOURCE_OBJECT       - lc__links_joins__daily_channel_brand_retailer
                        - trans__trans__daily_channel_brand_retailer
*/

WITH lc AS (
    SELECT
        *,
        'LOYALTY CARD' AS category
    FROM {{ ref('lc__links_joins__daily_channel_brand_retailer') }}
    WHERE channel IN ('LLOYDS', 'MIXR')
),

txns AS (
    SELECT
        *,
        'TRANSACTIONS' AS category
    FROM {{ ref('trans__trans__daily_channel_brand_retailer') }}
    WHERE channel IN ('LLOYDS', 'MIXR')
),

final AS (
    SELECT
        date,
        category,
        channel,
        brand,
        loyalty_plan_name,
        loyalty_plan_company,
        lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count,
        lc009__successful_loyalty_card_links__daily_channel_brand_retailer__count,
        lc021__successful_loyalty_card_joins__daily_channel_brand_retailer__dcount_user,
        lc017__successful_loyalty_card_links__daily_channel_brand_retailer__dcount_user,
        lc075__successful_loyalty_card_joins__daily_channel_brand_retailer__csum,
        lc079__successful_loyalty_card_links__daily_channel_brand_retailer__csum,
        NULL AS t067__spend__daily_channel_brand_retailer__csum,
        NULL AS t069__txns__daily_channel_brand_retailer__csum,
        NULL AS t073__spend__daily_channel_brand_retailer__sum,
        NULL AS t075__txns__daily_channel_brand_retailer__dcount
    FROM lc
    UNION ALL
    SELECT
        date,
        category,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        NULL
            AS lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count,
        NULL
            AS lc009__successful_loyalty_card_links__daily_channel_brand_retailer__count,
        NULL
            AS lc021__successful_loyalty_card_joins__daily_channel_brand_retailer__dcount_user,
        NULL
            AS lc017__successful_loyalty_card_links__daily_channel_brand_retailer__dcount_user,
        NULL
            AS lc075__successful_loyalty_card_joins__daily_channel_brand_retailer__csum,
        NULL
            AS lc079__successful_loyalty_card_links__daily_channel_brand_retailer__csum,
        t067__spend__daily_channel_brand_retailer__csum,
        t069__txns__daily_channel_brand_retailer__csum,
        t073__spend__daily_channel_brand_retailer__sum,
        t075__txns__daily_channel_brand_retailer__dcount
    FROM txns
)

SELECT *
FROM final
