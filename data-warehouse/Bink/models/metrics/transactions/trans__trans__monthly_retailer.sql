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
    FROM {{ ref('txns_trans') }})

   , dim_date AS (
    SELECT DISTINCT start_of_month, end_of_month
    FROM {{ ref('stg_metrics__dim_date') }}
    WHERE date >= (
        SELECT MIN(date)
        FROM txn_events)
      AND date <= CURRENT_DATE())

   , stage AS (
    SELECT user_ref
         , transaction_id
         , loyalty_plan_name
         , loyalty_plan_company
         , status
         , DATE_TRUNC('month', date) AS date
         , spend_amount
         , loyalty_card_id
    FROM txn_events)

   , txn_period AS (
    SELECT d.start_of_month                                                                 AS date
         , s.loyalty_plan_company
         , s.loyalty_plan_name
         , SUM(CASE WHEN status = 'TXNS' THEN s.spend_amount END)                           AS spend_amount_period_positive
         , SUM(CASE WHEN status = 'REFUND' THEN s.spend_amount END)                         AS refund_amount_period
         , COUNT(DISTINCT CASE WHEN status = 'BNPL' THEN transaction_id END)                AS count_bnpl_period
         , COUNT(DISTINCT CASE WHEN status = 'TXNS' THEN transaction_id END)                AS count_transaction_period
         , COUNT(DISTINCT CASE WHEN status = 'REFUND' THEN transaction_id END)              AS count_refund_period
    FROM stage s
             LEFT JOIN dim_date d ON d.start_of_month = DATE_TRUNC('month', s.date)
    GROUP BY d.start_of_month, s.loyalty_plan_company, s.loyalty_plan_name)

   , txn_cumulative AS (
    SELECT date
         , loyalty_plan_company
         , loyalty_plan_name
         , SUM(spend_amount_period_positive) OVER (PARTITION BY loyalty_plan_company ORDER BY date) AS cumulative_spend
         , SUM(refund_amount_period) OVER (PARTITION BY loyalty_plan_company ORDER BY date)         AS cumulative_refund
         , SUM(count_bnpl_period)
               OVER (PARTITION BY loyalty_plan_company ORDER BY date)                               AS cumulative_bnpl_txns
         , SUM(count_transaction_period) OVER (PARTITION BY loyalty_plan_company ORDER BY date)     AS cumulative_txns
         , SUM(count_refund_period)
               OVER (PARTITION BY loyalty_plan_company ORDER BY date)                               AS cumulative_refund_txns
    FROM txn_period)

   , combine_all AS (
    SELECT COALESCE(s.date, p.date)                                 AS date
         , COALESCE(s.loyalty_plan_company, p.loyalty_plan_company) AS loyalty_plan_company
         , COALESCE(s.loyalty_plan_name, p.loyalty_plan_name)       AS loyalty_plan_name
         , COALESCE(s.cumulative_spend, 0)                          AS t004__spend__monthly_retailer__csum
         , COALESCE(s.cumulative_refund, 0)                         AS t005__refund__monthly_retailer__csum
         , COALESCE(s.cumulative_txns, 0)                           AS t006__txns__monthly_retailer__csum
         , COALESCE(s.cumulative_refund_txns, 0)                    AS t007__refund__monthly_retailer__csum
         , COALESCE(s.cumulative_bnpl_txns, 0)                      AS t008__bnpl_txns__monthly_retailer__csum
         , COALESCE(p.spend_amount_period_positive, 0)              AS t009__spend__monthly_retailer__sum
         , COALESCE(p.refund_amount_period, 0)                      AS t010__refund__monthly_retailer__sum
         , COALESCE(p.count_transaction_period, 0)                  AS t011__txns__monthly_retailer__dcount
         , COALESCE(p.count_refund_period, 0)                       AS t012__refund__monthly_retailer__dcount
         , COALESCE(p.count_bnpl_period, 0)                         AS t013__bnpl_txns__monthly_retailer__dcount
    FROM txn_cumulative s
             FULL OUTER JOIN txn_period p ON s.date = p.date AND s.loyalty_plan_company = p.loyalty_plan_company)

SELECT *
FROM combine_all