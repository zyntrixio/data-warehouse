/*
Created by:         Christopher Mitchell 
Created date:       2023-06-07
Last modified by:   
Last modified date: 

Description:
    Rewrite of the LL table lc_joins_links_snapshot and lc_joins_links containing both snapshot and daily absolute data of all link and join journeys split by merchant.
Notes:
    This code can be made more efficient if the start is pushed to the trans__lbg_user code and that can be the source for the majority of the dashboards including user_loyalty_plan_snapshot and user_with_loyalty_cards
Parameters:
    source_object       - trans_trans
                        - stg_metrics__dim_date
*/

WITH user_events AS (
    SELECT *
    FROM {{ ref('txns_trans') }})

   , dim_date AS (
    SELECT DISTINCT start_of_month, end_of_month
    FROM {{ ref('stg_metrics__dim_date') }}
    WHERE date >= (
        SELECT MIN(from_date)
        FROM user_events)
      AND date <= CURRENT_DATE())

   , user_snap AS (
    SELECT d.start_of_month         AS date
         , u.loyalty_plan_company
         , COUNT(DISTINCT user_ref) AS u108_active_users_brand_retailer_monthly__pit
    FROM user_events u
             LEFT JOIN dim_date d
                       ON d.end_of_month >= DATE(u.from_date)
                           AND d.end_of_month < COALESCE(DATE(u.to_date), '9999-12-31')
    GROUP BY d.start_of_month, u.loyalty_plan_company
    HAVING date IS NOT NULL)

   , user_period AS (
    SELECT d.start_of_month                      AS date
         , u.loyalty_plan_company
         , COALESCE(COUNT(DISTINCT user_ref), 0) AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM user_events u
             LEFT JOIN dim_date d
                       ON d.start_of_month = DATE_TRUNC('month', u.from_date)
    GROUP BY d.start_of_month, u.loyalty_plan_company)

   , combine_all AS (
    SELECT COALESCE(s.date, p.date)                                             AS date
         , COALESCE(s.loyalty_plan_company, p.loyalty_plan_company)             AS loyalty_plan_company
         , COALESCE(s.u108_active_users_brand_retailer_monthly__pit, 0)         AS u108_active_users_brand_retailer_monthly__pit
         , COALESCE(p.u107_active_users_brand_retailer_monthly__dcount_user, 0) AS u107_active_users_brand_retailer_monthly__dcount_user
    FROM user_snap s
             FULL OUTER JOIN user_period p ON s.date = p.date AND s.loyalty_plan_company = p.loyalty_plan_company)

SELECT *
FROM combine_all