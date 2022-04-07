WITH final AS (
    SELECT
        *
    FROM
        {{ ref('stg_user') }}
)
SELECT
    *
FROM
    final