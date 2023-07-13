/*
Created by:         Christopher Mitchell 
Created date:       2023-07-03
Last modified by:   
Last modified date: 

Description:
    todo
Parameters:
    source_object       - src__fact_lc_add
*/
WITH lc_events AS (
    SELECT *
    FROM {{ ref('lc_trans') }})

   , dim_date AS (
    SELECT DISTINCT start_of_month, end_of_month
    FROM {{ ref('dim_date') }}
    WHERE date >= (
        SELECT MIN(from_date)
        FROM lc_events)
      AND date <= CURRENT_DATE())

   , count_up_snap AS (
    SELECT d.start_of_month                                                                       AS date
         , u.channel
         , u.brand
         , u.loyalty_plan_name
         , u.loyalty_plan_company
         , COALESCE(SUM(CASE WHEN event_type = 'SUCCESS' AND add_journey = 'JOIN' THEN 1 END), 0) AS join_success_state
         , COALESCE(SUM(CASE WHEN event_type = 'FAILED' AND add_journey = 'JOIN' THEN 1 END), 0)  AS join_failed_state
         , COALESCE(SUM(CASE WHEN event_type = 'REQUEST' AND add_journey = 'JOIN' THEN 1 END), 0) AS join_pending_state
         , COALESCE(SUM(CASE WHEN event_type = 'REMOVED' AND add_journey = 'JOIN' THEN 1 END), 0) AS join_removed_state

         , COALESCE(SUM(CASE WHEN event_type = 'SUCCESS' AND add_journey = 'LINK' THEN 1 END), 0) AS link_success_state
         , COALESCE(SUM(CASE WHEN event_type = 'FAILED' AND add_journey = 'LINK' THEN 1 END), 0)  AS link_failed_state
         , COALESCE(SUM(CASE WHEN event_type = 'REQUEST' AND add_journey = 'LINK' THEN 1 END), 0) AS link_pending_state
         , COALESCE(SUM(CASE WHEN event_type = 'REMOVED' AND add_journey = 'LINK' THEN 1 END), 0) AS link_removed_state
    FROM lc_events u
             LEFT JOIN dim_date d
                       ON d.end_of_month >= DATE(u.from_date)
                           AND d.end_of_month < COALESCE(DATE(u.to_date), '9999-12-31')
    GROUP BY d.start_of_month
           , u.brand
           , u.channel
           , u.loyalty_plan_name
           , u.loyalty_plan_company
    HAVING date IS NOT NULL)

   , count_up_abs AS (
    SELECT d.start_of_month                                                                       AS date
         , u.channel
         , u.brand
         , u.loyalty_plan_name
         , u.loyalty_plan_company
         , COALESCE(SUM(CASE WHEN event_type = 'REQUEST' AND add_journey = 'JOIN' THEN 1 END), 0) AS join_requests
         , COALESCE(SUM(CASE WHEN event_type = 'FAILED' AND add_journey = 'JOIN' THEN 1 END),0)                                                                            AS join_fails
         , COALESCE(SUM(CASE WHEN event_type = 'SUCCESS' AND add_journey = 'JOIN' THEN 1 END),0)                                                                            AS join_successes
         , COALESCE(SUM(CASE WHEN event_type = 'REMOVED' AND add_journey = 'JOIN' THEN 1 END),0)                                                                            AS join_deletes
         , COALESCE(SUM(CASE WHEN event_type = 'SUCCESS' AND add_journey = 'JOIN' AND CONSENT_RESPONSE THEN 1 END),0)                                                        AS join_successes_mrkt_opt_in

         , COALESCE(SUM(CASE WHEN event_type = 'REQUEST' AND add_journey = 'LINK' THEN 1 END),0)                                                                            AS link_requests
         , COALESCE(SUM(CASE WHEN event_type = 'FAILED' AND add_journey = 'LINK' THEN 1 END),0)                                                                            AS link_fails
         , COALESCE(SUM(CASE WHEN event_type = 'SUCCESS' AND add_journey = 'LINK' THEN 1 END),0)                                                                            AS link_successes
         , COALESCE(SUM(CASE WHEN event_type = 'REMOVED' AND add_journey = 'LINK' THEN 1 END),0)                                                                            AS link_deletes

         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REQUEST' AND add_journey = 'JOIN' THEN u.user_ref END),0)                                        AS join_requests_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'FAILED' AND add_journey = 'JOIN' THEN u.user_ref END),0)                                         AS join_fails_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'SUCCESS' AND add_journey = 'JOIN' THEN u.user_ref END),0)                                        AS join_successes_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REMOVED' AND add_journey = 'JOIN' THEN u.user_ref END),0)                                        AS join_deletes_unique_users

         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REQUEST' AND add_journey = 'LINK' THEN u.user_ref END),0)                                        AS link_requests_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'FAILED' AND add_journey = 'LINK' THEN u.user_ref END),0)                                         AS link_fails_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'SUCCESS' AND add_journey = 'LINK' THEN u.user_ref END),0)                                        AS link_successes_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REMOVED' AND add_journey = 'LINK' THEN u.user_ref END),0)                                        AS link_deletes_unique_users

         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REQUEST' THEN u.user_ref END),0)                                                                            AS requests_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'FAILED' THEN u.user_ref END),0)                                                                            AS fails_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'SUCCESS' THEN u.user_ref END),0)                                                                            AS successes_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REMOVED' THEN u.user_ref END),0)                                                                            AS deletes_unique_users

    FROM lc_events u
             LEFT JOIN dim_date d
                       ON d.start_of_month = DATE_TRUNC('month', u.from_date)
    GROUP BY d.start_of_month
           , u.brand
           , u.channel
           , u.loyalty_plan_name
           , u.loyalty_plan_company
    HAVING start_of_month IS NOT NULL)

,   adding_cumulative_abs AS (

    SELECT
        date
        ,channel
        ,brand
        ,loyalty_plan_name
        ,loyalty_plan_company

         ,join_requests
         ,join_fails
         ,join_successes
         ,join_successes_mrkt_opt_in
         ,join_deletes
         ,link_requests
         ,link_fails
         ,link_successes
         ,link_deletes

         ,SUM(join_requests) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)              AS join_requests_cumulative
         ,SUM(join_fails) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)                 AS join_fails_cumulative
         ,SUM(join_successes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)             AS join_successes_cumulative
         ,SUM(join_successes_mrkt_opt_in) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC) AS join_successes_mrkt_opt_in_cumulative
         ,SUM(join_deletes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)               AS join_deletes_cumulative
         ,SUM(link_requests) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)              AS link_requests_cumulative
         ,SUM(link_fails) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)                 AS link_fails_cumulative
         ,SUM(link_successes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)             AS link_successes_cumulative
         ,SUM(link_deletes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND, LOYALTY_PLAN_NAME, CHANNEL ORDER BY DATE ASC)               AS link_deletes_cumulative

         , join_requests_unique_users
        , join_fails_unique_users
        , join_successes_unique_users
        , join_deletes_unique_users
        , link_requests_unique_users
        , link_fails_unique_users
        , link_successes_unique_users
        , link_deletes_unique_users
        
    FROM count_up_abs
)

   , all_together AS (
    SELECT COALESCE(a.date, s.date)                                 date
         , COALESCE(a.brand, s.brand)                               brand
         , COALESCE(a.channel, s.channel)                           channel
         , COALESCE(a.loyalty_plan_name, s.loyalty_plan_name)       loyalty_plan_name
         , COALESCE(a.loyalty_plan_company, s.loyalty_plan_company) loyalty_plan_company

         , COALESCE(s.join_success_state, 0)          AS            join_success_state
         , COALESCE(s.join_failed_state, 0)           AS            join_failed_state
         , COALESCE(s.join_pending_state, 0)          AS            join_pending_state
         , COALESCE(s.join_removed_state, 0)          AS            join_removed_state
         , COALESCE(s.link_success_state, 0)          AS            link_success_state
         , COALESCE(s.link_failed_state, 0)           AS            link_failed_state
         , COALESCE(s.link_pending_state, 0)          AS            link_pending_state
         , COALESCE(s.link_removed_state, 0)          AS            link_removed_state

         , COALESCE(a.join_requests, 0)               AS            join_requests
         , COALESCE(a.join_fails, 0)                  AS            join_fails
         , COALESCE(a.join_successes, 0)              AS            join_successes
         , COALESCE(a.join_successes_mrkt_opt_in, 0)  AS            join_successes_mrkt_opt_in
         , COALESCE(a.join_deletes, 0)                AS            join_deletes
         , COALESCE(a.link_requests, 0)               AS            link_requests
         , COALESCE(a.link_fails, 0)                  AS            link_fails
         , COALESCE(a.link_successes, 0)              AS            link_successes
         , COALESCE(a.link_deletes, 0)                AS            link_deletes

        ,COALESCE(a.join_requests_cumulative, 0)                AS join_requests_cumulative
        ,COALESCE(a.join_fails_cumulative, 0)                   AS join_fails_cumulative
        ,COALESCE(a.join_successes_cumulative, 0)               AS join_successes_cumulative
        ,COALESCE(a.join_successes_mrkt_opt_in_cumulative, 0)   AS join_successes_mrkt_opt_in_cumulative
        ,COALESCE(a.join_deletes_cumulative, 0)                 AS join_deletes_cumulative
        ,COALESCE(a.link_requests_cumulative, 0)                AS link_requests_cumulative
        ,COALESCE(a.link_fails_cumulative, 0)                   AS link_fails_cumulative
        ,COALESCE(a.link_successes_cumulative, 0)               AS link_successes_cumulative
        ,COALESCE(a.link_deletes_cumulative, 0)                 AS link_deletes_cumulative

         , COALESCE(a.join_requests_unique_users, 0)  AS            join_requests_unique_users
         , COALESCE(a.join_fails_unique_users, 0)     AS            join_fails_unique_users
         , COALESCE(a.join_successes_unique_users, 0) AS            join_successes_unique_users
         , COALESCE(a.join_deletes_unique_users, 0)   AS            join_deletes_unique_users
         , COALESCE(a.link_requests_unique_users, 0)  AS            link_requests_unique_users
         , COALESCE(a.link_fails_unique_users, 0)     AS            link_fails_unique_users
         , COALESCE(a.link_successes_unique_users, 0) AS            link_successes_unique_users
         , COALESCE(a.link_deletes_unique_users, 0)   AS            link_deletes_unique_users
    FROM adding_cumulative_abs a
             FULL OUTER JOIN count_up_snap s
                             ON a.date = s.date AND a.brand = s.brand AND a.loyalty_plan_name = s.loyalty_plan_name
                             )

   , add_combine_rename AS (
    SELECT date
         , channel
         , brand
         , loyalty_plan_name
         , loyalty_plan_company

         , join_success_state + link_success_state AS LC300__SUCCESSFUL_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , join_pending_state + link_pending_state AS LC301__REQUESTS_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , join_failed_state + link_failed_state   AS LC302__FAILED_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , link_removed_state + join_removed_state AS LC303__DELETED_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , link_success_state                      AS LC304__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , link_pending_state                      AS LC305__REQUESTS_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , link_failed_state                       AS LC306__FAILED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , link_removed_state                      AS LC307__DELETED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , join_success_state                      AS LC308__SUCCESSFUL_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , join_pending_state                      AS LC309__REQUESTS_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , join_failed_state                       AS LC310__FAILED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT
         , join_removed_state                      AS LC311__DELETED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__PIT


         , join_successes + link_successes         AS LC312__SUCCESSFUL_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , join_requests + link_requests           AS LC313__REQUESTS_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , join_fails + link_fails                 AS LC314__FAILED_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , join_deletes + link_deletes             AS LC315__DELETED_LOYALTY_CARDS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , link_successes                          AS LC316__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , link_requests                           AS LC317__REQUESTS_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , link_fails                              AS LC318__FAILED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , link_deletes                            AS LC319__DELETED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , join_successes                          AS LC320__SUCCESSFUL_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , join_requests                           AS LC321__REQUESTS_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , join_fails                              AS LC322__FAILED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT
         , join_deletes                            AS LC323__DELETED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT

         , link_successes_unique_users             AS LC324__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , link_requests_unique_users              AS LC325__REQUESTS_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , link_fails_unique_users                 AS LC326__FAILED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , link_deletes_unique_users               AS LC327__DELETED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , join_successes_unique_users             AS LC328__SUCCESSFUL_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , join_requests_unique_users              AS LC329__REQUESTS_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , join_fails_unique_users                 AS LC330__FAILED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER
         , join_deletes_unique_users               AS LC331__DELETED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__DCOUNT_USER

        , join_successes_cumulative                 AS  LC367__SUCCESSFUL_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
        , join_requests_cumulative                  AS  LC368__REQUESTS_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
        , join_fails_cumulative                     AS  LC369__FAILED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
        , join_deletes_cumulative                   AS  LC370__DELETED_LOYALTY_CARD_LINKS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
        , link_successes_cumulative                 AS  LC371__SUCCESSFUL_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
        , link_requests_cumulative                  AS  LC372__REQUESTS_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
        , link_fails_cumulative                     AS  LC373__FAILED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
        , link_deletes_cumulative                   AS  LC374__DELETED_LOYALTY_CARD_JOINS__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM

         , join_successes_mrkt_opt_in_cumulative     AS LC383__SUCESSFUL_LOYALTY_CARD_JOIN_MRKT_OPT_IN__MONTHLY_CHANNEL_BRAND_RETAILER__CSUM
         , join_successes_mrkt_opt_in                AS LC384__SUCESSFUL_LOYALTY_CARD_JOIN_MRKT_OPT_IN__MONTHLY_CHANNEL_BRAND_RETAILER__COUNT

    FROM all_together)

SELECT *
FROM add_combine_rename
