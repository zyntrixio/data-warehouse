/*
Created by:         Christopher Mitchell
Created date:       2023-07-18
Last modified by:   
Last modified date: 

Description:
    Rewrite of metrics for transactions at a monthly agg
Notes:
    source_object       - trans__trans__monthly
                        - dim_date?
*/

WITH avg_events AS (
    SELECT *
    FROM {{ ref('trans_trans') }})

   , stage AS (
    SELECT DATE(date)                                 AS date
         , loyalty_plan_company
         , t005_spend__monthly_retailer__sum          AS period_spend
         , period_user
         , t004_transactions__monthly_retailer__count AS period_txn
         , t009_refund__monthly_retailer__count       AS period_refund
    FROM avg_events)

   , agg AS (
    SELECT date
         , loyalty_plan_company
         , DIV0(period_spend, period_user) AS arpu
         , DIV0(period_txn, period_user)   AS atf
         , DIV0(period_spend, period_txn)  AS aov
    FROM stage)

SELECT *
FROM agg