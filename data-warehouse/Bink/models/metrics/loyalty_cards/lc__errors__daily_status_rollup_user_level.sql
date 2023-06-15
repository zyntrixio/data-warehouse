/*
Created by:         Anand Bhakta
Created date:       2023-06-07
Last modified by:   
Last modified date: 

Description:
    data source for errors broken down daily and by user / status rollup. 
Parameters:
    source_object       - lc__errors__daily_user_level
                        - lc__links_joins__daily_retailer_channel
*/

WITH lc_errors AS (
    SELECT *
    FROM {{ref('lc_status_trans')}}
    WHERE STATUS_TYPE = 'Error'
)

,errors_aggregate AS (
    SELECT
        DATE(STATUS_START_TIME)                                 AS DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,STATUS_ROLLUP
        ,IS_RESOLVED
        ,CONCAT(EXTERNAL_USER_REF, LOYALTY_PLAN_COMPANY)        AS LC_USER_REF
    FROM
        lc_errors
)

,errors_metrics AS (
    SELECT
        DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,STATUS_ROLLUP
        ,MAX(LC_USER_REF)                                       AS LC101__ERROR_LOYALTY_CARDS__DAILY_USER_LEVEL__UID
        ,MAX(CASE 
            WHEN IS_RESOLVED 
            THEN LC_USER_REF
        END)                                                    AS LC102__RESOLVED_ERROR_LOYALTY_CARDS__DAILY_USER_LEVEL__UID
        ,COUNT(*)                                               AS LC103__ERROR_VISITS__DAILY_USER_LEVEL__COUNT
    FROM
        errors_aggregate
    GROUP BY
        DATE
        ,LOYALTY_PLAN_COMPANY
        ,CHANNEL
        ,BRAND
        ,STATUS_ROLLUP
        ,LC_USER_REF
)

select * from errors_metrics
