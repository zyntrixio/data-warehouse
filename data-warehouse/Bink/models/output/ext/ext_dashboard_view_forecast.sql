/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-12
Last modified by: Anand Bhakta
Last modified date: 2023-12-19

DESCRIPTION:
    OUTPUT LAYER FOR EXT DASHBOARD
PARAMETERS:
    SOURCE_OBJECT       - lc__links_joins__daily_channel_brand_retailer__forecast
                        - trans__trans__daily_channel_brand_retailer__forecast
*/

WITH lc AS (
    SELECT
        *,
        'LOYALTY CARD' AS category
    FROM {{ ref('lc__links_joins__daily_channel_brand_retailer__forecast') }}
    WHERE channel IN ('LLOYDS', 'MIXR')
),

txns AS (
    SELECT
        *,
        'TRANSACTIONS' AS category
    FROM {{ ref('trans__trans__daily_channel_brand_retailer__forecast') }}
    WHERE channel IN ('LLOYDS', 'MIXR')
),

final AS (
    SELECT
        date,
        category,
        loyalty_plan_company,
        loyalty_plan_name,
        channel,
        brand,
        lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count__forecast,
        lc075__successful_loyalty_card_joins__daily_channel_brand_retailer__csum__forecast,
        NULL AS t073__spend__daily_channel_brand_retailer__sum__forecast,
        NULL AS t067__spend__daily_channel_brand_retailer__csum__forecast
    FROM lc
    UNION ALL
    SELECT
        date,
        category,
        loyalty_plan_company,
        loyalty_plan_name,
        channel,
        brand,
        NULL
            AS lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count__forecast,
        NULL
            AS lc075__successful_loyalty_card_joins__daily_channel_brand_retailer__csum__forecast,
        t073__spend__daily_channel_brand_retailer__sum__forecast,
        t067__spend__daily_channel_brand_retailer__csum__forecast
    FROM txns
)

SELECT *
FROM final
