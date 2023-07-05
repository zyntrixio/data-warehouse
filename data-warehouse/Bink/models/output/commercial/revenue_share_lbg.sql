/*
Created by:          Christopher Mitchell
Created date:        2023-07-04
Last modified by:   
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - users_overview
Parameters:
    source_object       - lc__links_joins__monthly_retailer_channel
                        - User__transactions__monthly_user_level
*/

WITH joins AS (
    SELECT *
        , 'JOINS' AS TAB
    FROM {{ ref('lc__links_joins__monthly_retailer_channel') }}
    WHERE CHANNEL = 'LLOYDS'
    AND LOYALTY_PLAN_COMPANY NOT IN ('Loyalteas', 'Bink Sweet Shop')
    )

    , active AS (
        SELECT *
            ,'ACTIVE_USERS' AS TAB
        FROM {{ ref('user__transactions__monthly_user_level') }}
        WHERE CHANNEL = 'LLOYDS'
        AND LOYALTY_PLAN_COMPANY NOT IN ('Loyalteas', 'Bink Sweet Shop')
    )

    , combine AS (
        SELECT
            TAB
            ,*
        FROM
            joins
        UNION ALL

        SELECT
            TAB
            ,*
        FROM
            active
    )

SELECT * FROM combine