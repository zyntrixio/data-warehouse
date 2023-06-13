/*
Created by:         Anand Bhakta
Created date:       2023-06-07
Last modified by:   
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - loyalty_cards_overview
Parameters:
    source_object       - lc__errors__daily_user_level
                        - lc__links_joins__daily_retailer_channel
*/

WITH lc_errors AS (
    SELECT *
    FROM {{ref('lc__errors__daily_status_rollup_user_level')}}
    WHERE CHANNEL = 'LLOYDS' AND STATUS_ROLLUP != 'System Issue'
)

,lc_core AS (
    SELECT *
    FROM {{ref('lc__links_joins__daily_retailer_channel')}}
    WHERE CHANNEL = 'LLOYDS'
)

,combine AS (
    SELECT
        DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,STATUS_ROLLUP
        ,LC101__ERROR_LOYALTY_CARDS__DAILY_USER_LEVEL__UID
        ,LC102__RESOLVED_ERROR_LOYALTY_CARDS__DAcouILY_USER_LEVEL__UID
        ,LC103__ERROR_VISITS__DAILY_USER_LEVEL__COUNT
        ,NULL AS LC006__REQUESTS_LOYALTY_CARDS__DAILY_CHANNEL_BRAND_RETAILER__COUNT
        ,NULL AS LC007__FAILED_LOYALTY_CARDS__DAILY_CHANNEL_BRAND_RETAILER__COUNT       
    FROM
        lc_errors

    UNION ALL

    SELECT
        DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,NULL AS STATUS_ROLLUP
        ,NULL AS LC101__ERROR_LOYALTY_CARDS__DAILY_USER_LEVEL__UID
        ,NULL AS LC102__RESOLVED_ERROR_LOYALTY_CARDS__DAILY_USER_LEVEL__UID
        ,NULL AS LC103__ERROR_VISITS__DAILY_USER_LEVEL__COUNT
        ,LC006__REQUESTS_LOYALTY_CARDS__DAILY_CHANNEL_BRAND_RETAILER__COUNT
        ,LC007__FAILED_LOYALTY_CARDS__DAILY_CHANNEL_BRAND_RETAILER__COUNT
    FROM
        lc_core   
)

select * from combine
