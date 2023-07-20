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
        SELECT MIN(from_date)
        FROM txn_events)
      AND date <= CURRENT_DATE())

   , stage AS (
    SELECT user_ref
         , transaction_id
         , loyalty_plan_name
         , loyalty_plan_company
         , status
         , transaction_date
         , spend_amount
         , loyalty_card_id
         , from_date
         , to_date
    FROM txn_events)

   , txn_snap AS (
    SELECT d.start_of_month                                                                 AS date
         , s.loyalty_plan_company
         , SUM(CASE WHEN status = 'TXNS' THEN s.spend_amount END)                           AS spend_amount_snap_positive
         , SUM(CASE WHEN status = 'REFUND' THEN s.spend_amount END)                         AS refund_amount_snap
         , COALESCE(COUNT(DISTINCT CASE WHEN status = 'BNPL' THEN transaction_id END), 0)   AS count_bnpl_snap
         , COALESCE(COUNT(DISTINCT CASE WHEN status = 'TXNS' THEN transaction_id END), 0)   AS count_transaction_snap
         , COALESCE(COUNT(DISTINCT CASE WHEN status = 'REFUND' THEN transaction_id END), 0) AS count_refund_snap
    FROM stage s
             LEFT JOIN dim_date d ON d.end_of_month >= DATE(s.from_date)
        AND d.end_of_month < COALESCE(DATE(s.to_date), '9999-12-31')
    GROUP BY d.start_of_month, s.loyalty_plan_company
    HAVING date IS NOT NULL)

   , txn_period AS (
    SELECT d.start_of_month                                                                 AS date
         , s.loyalty_plan_company
         , SUM(CASE WHEN status = 'TXNS' THEN s.spend_amount END)                           AS spend_amount_period_positive
         , SUM(CASE WHEN status = 'REFUND' THEN s.spend_amount END)                         AS refund_amount_period
         , COALESCE(COUNT(DISTINCT CASE WHEN status = 'BNPL' THEN transaction_id END), 0)   AS count_bnpl_period
         , COALESCE(COUNT(DISTINCT CASE WHEN status = 'TXNS' THEN transaction_id END), 0)   AS count_transaction_period
         , COALESCE(COUNT(DISTINCT CASE WHEN status = 'REFUND' THEN transaction_id END), 0) AS count_refund_period
    FROM stage s
             LEFT JOIN dim_date d ON d.start_of_month = DATE_TRUNC('month', s.from_date)
    GROUP BY d.start_of_month, s.loyalty_plan_company)

   , combine_all AS (
    SELECT COALESCE(s.date, p.date)                                 AS date
         , COALESCE(s.loyalty_plan_company, p.loyalty_plan_company) AS loyalty_plan_company
         , COALESCE(s.spend_amount_snap_positive, 0)                AS t004__spend__monthly_retailer__pit
         , COALESCE(s.refund_amount_snap, 0)                        AS t005__refund__monthly_retailer__pit
         , COALESCE(s.count_transaction_snap, 0)                    AS t006__txns__monthly_retailer__pit
         , COALESCE(s.count_refund_snap, 0)                         AS t007__refund__monthly_retailer__pit
         , COALESCE(s.count_bnpl_snap, 0)                           AS t008__bnpl_txns__monthly_retailer__pit
         , COALESCE(p.spend_amount_period_positive, 0)              AS t009__spend__monthly_retailer__sum
         , COALESCE(p.refund_amount_period, 0)                      AS t010__refund__monthly_retailer__sum
         , COALESCE(p.count_transaction_period, 0)                  AS t011__txns__monthly_retailer__dcount
         , COALESCE(p.count_refund_period, 0)                       AS t012__refund__monthly_retailer__dcount
         , COALESCE(p.count_bnpl_period, 0)                         AS t013__bnpl_txns__monthly_retailer__dcount
    FROM txn_snap s
             FULL OUTER JOIN txn_period p ON s.date = p.date AND s.loyalty_plan_company = p.loyalty_plan_company)

SELECT *
FROM combine_all