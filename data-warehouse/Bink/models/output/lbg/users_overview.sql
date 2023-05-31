/*
Created by:         Anand Bhakta
Created date:       2023-05-23
Last modified by:   Christopher Mitchell
Last modified date: 2023-05-31

Description:
    Datasource to produce lloyds mi dashboard - users_overview
Parameters:
    source_object       - lc__links_joins__daily_retailer_channel
                        - lc__links_joins__daily_channel
                        - trans__trans__daily_user_level
                        - user__registrations__daily__channel_brand 
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

,users_metrics AS (
    SELECT
        *
        ,'USERS' AS TAB
    FROM {{ref('user__registrations__daily__channel_brand')}}
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
        ,NULL AS LC001__LC_SUCCESS_CUMULATIVE_ -- NEED TO CREATE SUFFIX FOR THIS METRIC
        ,LC002__LC_REMOVED_CUMULATIVE
        ,LC003__LC_SUCCESS_PERIOD
        ,LC004__LC_REMOVE_PERIOD
        ,NULL AS USR001__DAILY_REGISTRATIONS_PERIOD
        ,NULL AS USR002__DAILY_DEREGISTRATIONS_PERIOD
        ,NULL AS USR003__DAILY_REGISTRATIONS_CUMULATIVE
        ,NULL AS USR004__DAILY_DEREGISTRATIONS_CUMULATIVE
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
        ,NULL AS LC001__LC_SUCCESS_CUMULATIVE
        ,LC001__LC_SUCCESS_CUMULATIVE AS LC001__LC_SUCCESS_CUMULATIVE_
        ,NULL AS LC002__LC_REMOVED_CUMULATIVE
        ,NULL AS LC003__LC_SUCCESS_PERIOD
        ,NULL AS LC004__LC_REMOVE_PERIOD
        ,NULL AS USR001__DAILY_REGISTRATIONS_PERIOD
        ,NULL AS USR002__DAILY_DEREGISTRATIONS_PERIOD
        ,NULL AS USR003__DAILY_REGISTRATIONS_CUMULATIVE
        ,NULL AS USR004__DAILY_DEREGISTRATIONS_CUMULATIVE
        ,NULL AS SPEND
        ,NULL AS ACTIVE_USERS
        ,NULL AS TRANSACTIONS
    FROM
        lc_metrics    

    UNION ALL

    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,BRAND
        ,LOYALTY_PLAN_COMPANY
        ,NULL AS LC001__LC_SUCCESS_CUMULATIVE
        ,NULL AS LC001__LC_SUCCESS_CUMULATIVE_
        ,NULL AS LC002__LC_REMOVED_CUMULATIVE
        ,NULL AS LC003__LC_SUCCESS_PERIOD
        ,NULL AS LC004__LC_REMOVE_PERIOD
        ,NULL AS USR001__DAILY_REGISTRATIONS_PERIOD
        ,NULL AS USR002__DAILY_DEREGISTRATIONS_PERIOD
        ,NULL AS USR003__DAILY_REGISTRATIONS_CUMULATIVE
        ,NULL AS USR004__DAILY_DEREGISTRATIONS_CUMULATIVE
        ,SPEND
        ,ACTIVE_USERS
        ,TRANSACTIONS
    FROM    
        trans

    UNION ALL

    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,BRAND
        ,NULL AS LOYALTY_PLAN_COMPANY
        ,NULL AS LC001__LC_SUCCESS_CUMULATIVE
        ,NULL AS LC001__LC_SUCCESS_CUMULATIVE_
        ,NULL AS LC002__LC_REMOVED_CUMULATIVE
        ,NULL AS LC003__LC_SUCCESS_PERIOD
        ,NULL AS LC004__LC_REMOVE_PERIOD
        ,USR001__DAILY_REGISTRATIONS_PERIOD
        ,USR002__DAILY_DEREGISTRATIONS_PERIOD
        ,USR003__DAILY_REGISTRATIONS_CUMULATIVE
        ,USR004__DAILY_DEREGISTRATIONS_CUMULATIVE
        ,NULL AS SPEND
        ,NULL AS ACTIVE_USERS
        ,NULL AS TRANSACTIONS
    FROM
        users_metrics
)


select * from metric_select