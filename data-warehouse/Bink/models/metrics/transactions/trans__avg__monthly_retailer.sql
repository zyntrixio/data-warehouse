/*
Created by:         Christopher Mitchell
Created date:       2023-07-18
Last modified by:   
Last modified date: 

Description:
    Rewrite of metrics for transactions at a monthly agg
Notes:
    source_object       - trans__trans__monthly_retailer
                        - user__transactions__monthly_retailer
*/
WITH trans_events AS (
    SELECT *
    FROM {{ ref('trans__trans__monthly_retailer') }})

   , user_events AS (
    SELECT *
    FROM {{ ref('user__transactions__monthly_retailer') }})

   , joins AS (
    SELECT t.date
         , t.loyalty_plan_company
         , t.loyalty_plan_name
         , t.t009__spend__monthly_retailer__sum
         , t.t010__refund__monthly_retailer__sum
         , t.t011__txns__monthly_retailer__dcount
         , t.t012__refund__monthly_retailer__dcount
         , t.t013__bnpl_txns__monthly_retailer__dcount
         , u.U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID
    FROM trans_events t
             LEFT JOIN user_events u ON u.loyalty_plan_company = t.loyalty_plan_company AND u.date = t.date)

   , aggs AS (
    SELECT date
         , loyalty_plan_company
         , loyalty_plan_name
         , DIV0(t009__spend__monthly_retailer__sum, t011__txns__monthly_retailer__dcount) AS T014__AOV__MONTHLY_RETAILER__AVG
         , DIV0(t009__spend__monthly_retailer__sum,
                U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID)                    AS T015__ARPU__MONTHLY_RETAILER__AVG
         , DIV0(t011__txns__monthly_retailer__dcount,
                U107_ACTIVE_USERS_BRAND_RETAILER_MONTHLY__DCOUNT_UID)                    AS T016__ATF__MONTHLY_RETAILER__AVG
    FROM joins)

SELECT *
FROM aggs