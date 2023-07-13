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
           , u.loyalty_plan_name
           , u.loyalty_plan_company
    HAVING date IS NOT NULL)

   , count_up_abs AS (
    SELECT d.start_of_month                                                                       AS date
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
           , u.loyalty_plan_name
           , u.loyalty_plan_company
    HAVING start_of_month IS NOT NULL)

,   adding_cumulative_abs AS (

    SELECT
        date
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

         ,SUM(join_requests) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)              AS join_requests_cumulative
         ,SUM(join_fails) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)                 AS join_fails_cumulative
         ,SUM(join_successes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)             AS join_successes_cumulative
         ,SUM(join_successes_mrkt_opt_in) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC) AS join_successes_mrkt_opt_in_cumulative
         ,SUM(join_deletes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)               AS join_deletes_cumulative
         ,SUM(link_requests) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)              AS link_requests_cumulative
         ,SUM(link_fails) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)                 AS link_fails_cumulative
         ,SUM(link_successes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)             AS link_successes_cumulative
         ,SUM(link_deletes) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, LOYALTY_PLAN_NAME ORDER BY DATE ASC)               AS link_deletes_cumulative

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
                             ON a.date = s.date AND a.loyalty_plan_name = s.loyalty_plan_name
                             )

   , add_combine_rename AS (
    SELECT 
            date
         , loyalty_plan_name
         , loyalty_plan_company

         , join_success_state                      AS lc061__successful_loyalty_card_joins__monthly_retailer__pit
         , join_failed_state                       AS lc063__failed_loyalty_card_joins__monthly_retailer__pit
         , join_pending_state                      AS lc062__requests_loyalty_card_joins__monthly_retailer__pit
         , join_removed_state                      AS lc064__deleted_loyalty_card_joins__monthly_retailer__pit
         , link_success_state                      AS lc057__successful_loyalty_card_links__monthly_retailer__pit
         , link_failed_state                       AS lc059__failed_loyalty_card_links__monthly_retailer__pit
         , link_pending_state                      AS lc058__requests_loyalty_card_links__monthly_retailer__pit
         , link_removed_state                      AS lc060__deleted_loyalty_card_links__monthly_retailer__pit
         , join_success_state + link_success_state AS lc046__successful_loyalty_cards__monthly_retailer__pit
         , join_pending_state + link_pending_state AS lc047__requests_loyalty_cards__monthly_retailer__pit
         , join_failed_state + link_failed_state   AS lc045__failed_loyalty_cards__monthly_retailer__pit
         , link_removed_state + join_removed_state AS lc048__deleted_loyalty_cards__monthly_retailer__pit

         , join_requests                           AS lc046__requests_loyalty_card_joins__monthly_retailer__count
         , join_fails                              AS lc047__failed_loyalty_card_joins__monthly_retailer__count
         , join_successes                          AS lc045__successful_loyalty_card_joins__monthly_retailer__count
         , join_successes_mrkt_opt_in              AS lc065__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count
         , join_deletes                            AS lc048__deleted_loyalty_card_joins__monthly_retailer__count
         , link_requests                           AS lc042__requests_loyalty_card_links__monthly_retailer__count
         , link_fails                              AS lc043__failed_loyalty_card_links__monthly_retailer__count
         , link_successes                          AS lc041__successful_loyalty_card_links__monthly_retailer__count
         , link_deletes                            AS lc044__deleted_loyalty_card_links__monthly_retailer__count
         , join_requests + link_requests           AS lc038__requests_loyalty_cards__monthly_retailer__count
         , join_fails + link_fails                 AS lc039__failed_loyalty_cards__monthly_retailer__count
         , join_successes + link_successes         AS lc037__successful_loyalty_cards__monthly_retailer__count
         , join_deletes + link_deletes             AS lc040__deleted_loyalty_cards__monthly_retailer__count

        , join_requests_cumulative                  AS lc066__requests_loyalty_card_joins__monthly_retailer__csum
        , join_fails_cumulative                     AS lc067__failed_loyalty_card_joins__monthly_retailer__csum
        , join_successes_cumulative                 AS lc068__successful_loyalty_card_joins__monthly_retailer__csum
        , join_successes_mrkt_opt_in_cumulative     AS lc069__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__csum
        , join_deletes_cumulative                   AS lc070__deleted_loyalty_card_joins__monthly_retailer__csum
        , link_requests_cumulative                  AS lc071__requests_loyalty_card_links__monthly_retailer__csum
        , link_fails_cumulative                     AS lc072__failed_loyalty_card_links__monthly_retailer__csum
        , link_successes_cumulative                 AS lc073__successful_loyalty_card_links__monthly_retailer__csum
        , link_deletes_cumulative                   AS lc074__deleted_loyalty_card_links__monthly_retailer__csum

         , join_requests_unique_users              AS lc054__requests_loyalty_card_joins__monthly_retailer__dcount_user
         , join_fails_unique_users                 AS lc055__failed_loyalty_card_joins__monthly_retailer__dcount_user
         , join_successes_unique_users             AS lc053__successful_loyalty_card_joins__monthly_retailer__dcount_user
         , join_deletes_unique_users               AS lc056__deleted_loyalty_card_joins__monthly_retailer__dcount_user
         , link_requests_unique_users              AS lc050__requests_loyalty_card_links__monthly_retailer__dcount_user
         , link_fails_unique_users                 AS lc051__failed_loyalty_card_links__monthly_retailer__dcount_user
         , link_successes_unique_users             AS lc049__successful_loyalty_card_links__monthly_retailer__dcount_user
         , link_deletes_unique_users               AS lc052__deleted_loyalty_card_links__monthly_retailer__dcount_user

    FROM all_together)

SELECT *
FROM add_combine_rename
