/*
Created by:         Christopher Mitchell
Created date:       2023-07-17
Last modified by:   
Last modified date: 

Description:
    Rewrite of metrics for transactions at a monthly agg
Notes:
    source_object       - fact transaction
*/

WITH txn_events AS (
    SELECT *
    FROM {{ ref('trans_trans') }})

   , dim_date AS (
    SELECT DISTINCT start_of_month, end_of_month
    FROM {{ ref('stg_metrics__dim_date') }}
    WHERE date >= (
        SELECT MIN(from_date)
        FROM txn_events)
      AND date <= CURRENT_DATE())

   , stage AS (
    SELECT user_ref
         , transaction_id
         , loyalty_plan_name
         , loyalty_plan_company
         , transaction_date
         , spend_amount
         , loyalty_card_id
         , from_date
         , to_date
    FROM txn_events)

   , txn_snap AS (
    SELECT d.start_of_month                                           AS date
         , s.loyalty_plan_company
         , SUM(CASE WHEN s.spend_amount >= 0 THEN s.spend_amount END) AS spend_amount_snap_positive
         , SUM(CASE WHEN s.spend_amount < 0 THEN s.spend_amount END)  AS refund_amount_snap
         , COUNT(s.user_ref)                                          AS count_user_snap
         , COUNT(DISTINCT s.transaction_id)                           AS dcount_transaction_snap
    FROM stage s
             LEFT JOIN dim_date d ON d.end_of_month >= DATE(s.from_date)
        AND d.end_of_month < COALESCE(DATE(s.to_date), '9999-12-31')
    GROUP BY d.start_of_month, s.loyalty_plan_company
    HAVING date IS NOT NULL)

   , txn_period AS (
    SELECT d.start_of_month                                           AS date
         , s.loyalty_plan_company
         , SUM(CASE WHEN s.spend_amount >= 0 THEN s.spend_amount END) AS spend_amount_period_positive
         , SUM(CASE WHEN s.spend_amount < 0 THEN s.spend_amount END)  AS refund_amount_period
         , COUNT(s.user_ref)                                          AS count_user_period
         , COUNT(DISTINCT s.transaction_id)                           AS dcount_transaction_period
    FROM stage s
             LEFT JOIN dim_date d ON d.end_of_month = DATE(s.from_date)
    GROUP BY d.start_of_month, s.loyalty_plan_company
    HAVING date IS NOT NULL)

   , combine_all AS (
    SELECT COALESCE(s.date, p.date)                                 AS DATE
         , COALESCE(s.loyalty_plan_company, p.loyalty_plan_company) AS LOYALTY_PLAN_COMPANY
         , COALESCE(s.spend_amount_snap_positive, 0)                AS T014_SPEND__MONTHLY_RETAILER__SUM
         , COALESCE(s.refund_amount_snap, 0)                        AS T015_REFUND__MONTHLY_RETAILER__SUM
         , COALESCE(s.count_user_snap, 0)                           AS cumulative_user
         , COALESCE(s.dcount_transaction_snap, 0)                   AS T010_TRANSACTIONS__MONTHLY_RETAILER__PIT
         , COALESCE(p.spend_amount_period_positive, 0)              AS T005_SPEND__MONTHLY_RETAILER__SUM
         , COALESCE(p.count_user_period, 0)                         AS period_user
         , COALESCE(p.dcount_transaction_period, 0)                 AS T004_TRANSACTIONS__MONTHLY_RETAILER__COUNT
         , COALESCE(p.refund_amount_period, 0)                      AS T009_REFUND__MONTHLY_RETAILER__COUNT
    FROM txn_snap s
             FULL OUTER JOIN txn_period p ON s.date = p.date AND s.loyalty_plan_company = p.loyalty_plan_company)
             
SELECT *
FROM combine_all