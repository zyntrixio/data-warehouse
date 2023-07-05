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
    FROM metrics_staging.transformation.lc_trans)

   , dim_date AS (
    SELECT DISTINCT start_of_month, end_of_month
    FROM prod.bink.dim_date
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
         , COALESCE(SUM(CASE WHEN event_type = 'FAILED' AND add_journey = 'JOIN' THEN 1 END),
                    0)                                                                            AS join_fails
         , COALESCE(SUM(CASE WHEN event_type = 'SUCCESS' AND add_journey = 'JOIN' THEN 1 END),
                    0)                                                                            AS join_successes
         , COALESCE(SUM(CASE WHEN event_type = 'REMOVED' AND add_journey = 'JOIN' THEN 1 END),
                    0)                                                                            AS join_deletes

         , COALESCE(SUM(CASE WHEN event_type = 'REQUEST' AND add_journey = 'LINK' THEN 1 END),
                    0)                                                                            AS link_requests
         , COALESCE(SUM(CASE WHEN event_type = 'FAILED' AND add_journey = 'LINK' THEN 1 END),
                    0)                                                                            AS link_fails
         , COALESCE(SUM(CASE WHEN event_type = 'SUCCESS' AND add_journey = 'LINK' THEN 1 END),
                    0)                                                                            AS link_successes
         , COALESCE(SUM(CASE WHEN event_type = 'REMOVED' AND add_journey = 'LINK' THEN 1 END),
                    0)                                                                            AS link_deletes

         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REQUEST' AND add_journey = 'JOIN' THEN u.user_ref END),
                    0)                                                                            AS join_requests_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'FAILED' AND add_journey = 'JOIN' THEN u.user_ref END),
                    0)                                                                            AS join_fails_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'SUCCESS' AND add_journey = 'JOIN' THEN u.user_ref END),
                    0)                                                                            AS join_successes_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REMOVED' AND add_journey = 'JOIN' THEN u.user_ref END),
                    0)                                                                            AS join_deletes_unique_users

         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REQUEST' AND add_journey = 'LINK' THEN u.user_ref END),
                    0)                                                                            AS link_requests_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'FAILED' AND add_journey = 'LINK' THEN u.user_ref END),
                    0)                                                                            AS link_fails_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'SUCCESS' AND add_journey = 'LINK' THEN u.user_ref END),
                    0)                                                                            AS link_successes_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REMOVED' AND add_journey = 'LINK' THEN u.user_ref END),
                    0)                                                                            AS link_deletes_unique_users

         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REQUEST' THEN u.user_ref END),
                    0)                                                                            AS requests_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'FAILED' THEN u.user_ref END),
                    0)                                                                            AS fails_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'SUCCESS' THEN u.user_ref END),
                    0)                                                                            AS successes_unique_users
         , COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'REMOVED' THEN u.user_ref END),
                    0)                                                                            AS deletes_unique_users

    FROM lc_events u
             LEFT JOIN dim_date d
                       ON d.start_of_month = DATE_TRUNC('month', u.from_date)
    GROUP BY d.start_of_month
           , u.brand
           , u.channel
           , u.loyalty_plan_name
           , u.loyalty_plan_company
    HAVING start_of_month IS NOT NULL)

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
         , COALESCE(a.join_deletes, 0)                AS            join_deletes
         , COALESCE(a.link_requests, 0)               AS            link_requests
         , COALESCE(a.link_fails, 0)                  AS            link_fails
         , COALESCE(a.link_successes, 0)              AS            link_successes
         , COALESCE(a.link_deletes, 0)                AS            link_deletes

         , COALESCE(a.join_requests_unique_users, 0)  AS            join_requests_unique_users
         , COALESCE(a.join_fails_unique_users, 0)     AS            join_fails_unique_users
         , COALESCE(a.join_successes_unique_users, 0) AS            join_successes_unique_users
         , COALESCE(a.join_deletes_unique_users, 0)   AS            join_deletes_unique_users
         , COALESCE(a.link_requests_unique_users, 0)  AS            link_requests_unique_users
         , COALESCE(a.link_fails_unique_users, 0)     AS            link_fails_unique_users
         , COALESCE(a.link_successes_unique_users, 0) AS            link_successes_unique_users
         , COALESCE(a.link_deletes_unique_users, 0)   AS            link_deletes_unique_users
    FROM count_up_abs a
             FULL OUTER JOIN count_up_snap s
                             ON a.date = s.date AND a.brand = s.brand AND a.loyalty_plan_name = s.loyalty_plan_name)

   , add_combine_rename AS (
    SELECT date
         , channel
         , brand
         , loyalty_plan_name
         , loyalty_plan_company

         , join_success_state                      AS lc061__successful_loyalty_card_joins__monthly_channel_brand_retailer__pit
         , join_failed_state                       AS lc063__failed_loyalty_card_joins__monthly_channel_brand_retailer__pit
         , join_pending_state                      AS lc062__requests_loyalty_card_joins__monthly_channel_brand_retailer__pit
         , join_removed_state                      AS lc064__deleted_loyalty_card_joins__monthly_channel_brand_retailer__pit
         , link_success_state                      AS lc057__successful_loyalty_card_links__monthly_channel_brand_retailer__pit
         , link_failed_state                       AS lc059__failed_loyalty_card_links__monthly_channel_brand_retailer__pit
         , link_pending_state                      AS lc058__requests_loyalty_card_links__monthly_channel_brand_retailer__pit
         , link_removed_state                      AS lc060__deleted_loyalty_card_links__monthly_channel_brand_retailer__pit
         , join_success_state + link_success_state AS lc046__successful_loyalty_cards__monthly_channel_brand_retailer__pit
         , join_pending_state + link_pending_state AS lc047__requests_loyalty_cards__monthly_channel_brand_retailer__pit
         , join_failed_state + link_failed_state   AS lc045__failed_loyalty_cards__monthly_channel_brand_retailer__pit
         , link_removed_state + join_removed_state AS lc048__deleted_loyalty_cards__monthly_channel_brand_retailer__pit

         , join_requests                           AS lc046__requests_loyalty_card_joins__monthly_channel_brand_retailer__count
         , join_fails                              AS lc047__failed_loyalty_card_joins__monthly_channel_brand_retailer__count
         , join_successes                          AS lc045__successful_loyalty_card_joins__monthly_channel_brand_retailer__count
         , join_deletes                            AS lc048__deleted_loyalty_card_joins__monthly_channel_brand_retailer__count
         , link_requests                           AS lc042__requests_loyalty_card_links__monthly_channel_brand_retailer__count
         , link_fails                              AS lc043__failed_loyalty_card_links__monthly_channel_brand_retailer__count
         , link_successes                          AS lc041__successful_loyalty_card_links__monthly_channel_brand_retailer__count
         , link_deletes                            AS lc044__deleted_loyalty_card_links__monthly_channel_brand_retailer__count
         , join_requests + link_requests           AS lc038__requests_loyalty_cards__monthly_channel_brand_retailer__count
         , join_fails + link_fails                 AS lc039__failed_loyalty_cards__monthly_channel_brand_retailer__count
         , join_successes + link_successes         AS lc037__successful_loyalty_cards__monthly_channel_brand_retailer__count
         , join_deletes + link_deletes             AS lc040__deleted_loyalty_cards__monthly_channel_brand_retailer__count

         , join_requests_unique_users              AS lc054__requests_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
         , join_fails_unique_users                 AS lc055__failed_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
         , join_successes_unique_users             AS lc053__successful_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
         , join_deletes_unique_users               AS lc056__deleted_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
         , link_requests_unique_users              AS lc050__requests_loyalty_card_links__monthly_channel_brand_retailer__dcount_user
         , link_fails_unique_users                 AS lc051__failed_loyalty_card_links__monthly_channel_brand_retailer__dcount_user
         , link_successes_unique_users             AS lc049__successful_loyalty_card_links__monthly_channel_brand_retailer__dcount_user
         , link_deletes_unique_users               AS lc052__deleted_loyalty_card_links__monthly_channel_brand_retailer__dcount_user

    FROM all_together)

SELECT *
FROM add_combine_rename

to do update metric numbers