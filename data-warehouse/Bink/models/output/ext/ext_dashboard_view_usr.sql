/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-12
LAST MODIFIED BY:
LAST MODIFIED DATE:

DESCRIPTION:
    OUTPUT LAYER FOR EXT DASHBOARD
PARAMETERS:
    SOURCE_OBJECT       - user__transactions__daily_user_level
*/

WITH lc AS (
    SELECT
        *,
        'USER' AS category
    FROM {{ ref('user__transactions__daily_user_level') }}
    WHERE channel IN ('LLOYDS', 'MIXR')
),

final AS (
    SELECT
        date,
        channel,
        brand,
        loyalty_plan_company,
        u007__active_users__user_level_daily__uid,
        category
    FROM lc
)

SELECT *
FROM final
