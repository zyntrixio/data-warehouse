/*
Created by:         Anand Bhakta
Created date:       2023-05-23
Last modified by:   
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - users_overview
Parameters:
    source_object       - src__fact_lc_add
                        - src__fact_lc_removed
                        - src__dim_loyalty_card
                        - src__dim_date
*/

WITH lc_metrics_retailer AS (
    SELECT 
        *
        ,'LC_RETAILER_CHANNEL' AS TAB
    FROM {{ref('lc__links_joins__daily_retailer_channel')}}
    WHERE CHANNEL = 'LLOYDS'
)

,lc_metrics AS (
    SELECT 
        *
        ,'LC_CHANNEL' AS TAB
    FROM {{ref('lc__links_joins__daily_channel')}}
    WHERE CHANNEL = 'LLOYDS'
)

,trans AS (
    SELECT
        *
        ,'TRANS' AS TAB
    FROM {{ref('trans__trans__daily_user_level')}}
    WHERE CHANNEL = 'LLOYDS'
)

,metric_select AS (
    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,LC001__LC_SUCCESS_CUMULATIVE
        ,LC002__LC_REMOVED_CUMULATIVE
        ,LC003__LC_SUCCESS_PERIOD
        ,LC004__LC_REMOVE_PERIOD
        ,NULL AS SPEND
        ,NULL AS ACTIVE_USERS
        ,NULL AS TRANSACTIONS
    FROM
        lc_metrics_retailer

    UNION ALL

    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,BRAND
        ,NULL AS LOYALTY_PLAN_COMPANY
        ,LC001__LC_SUCCESS_CUMULATIVE
        ,LC002__LC_REMOVED_CUMULATIVE
        ,LC003__LC_SUCCESS_PERIOD
        ,LC004__LC_REMOVE_PERIOD
        ,NULL AS SPEND
        ,NULL AS ACTIVE_USERS
        ,NULL AS TRANSACTIONS
    FROM
        lc_metrics_retailer    

    UNION ALL

    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,NULL AS LC001__LC_SUCCESS_CUMULATIVE
        ,NULL AS LC002__LC_REMOVED_CUMULATIVE
        ,NULL AS LC003__LC_SUCCESS_PERIOD
        ,NULL AS LC004__LC_REMOVE_PERIOD
        ,SPEND
        ,ACTIVE_USERS
        ,TRANSACTIONS
    FROM    
        trans
)


select * from metric_select
