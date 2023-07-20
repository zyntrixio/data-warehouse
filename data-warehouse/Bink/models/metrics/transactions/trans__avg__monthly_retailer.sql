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
         , t.t009__spend__monthly_retailer__sum
         , t.t010__refund__monthly_retailer__sum
         , t.t011__txns__monthly_retailer__dcount
         , t.t012__refund__monthly_retailer__dcount
         , t.t013__bnpl_txns__monthly_retailer__dcount
         , u.u107_active_users_brand_retailer_monthly__dcount_user
    FROM trans_events t
             LEFT JOIN user_events u ON u.loyalty_plan_company = t.loyalty_plan_company AND u.date = t.date)

   , aggs AS (
    SELECT date
         , loyalty_plan_company
         , DIV0(t009__spend__monthly_retailer__sum, t011__txns__monthly_retailer__dcount) AS t014__aov__monthly_retailer
         , DIV0(t009__spend__monthly_retailer__sum,
                u107_active_users_brand_retailer_monthly__dcount_user)                    AS t015__arpu__monthly_retailer
         , DIV0(t011__txns__monthly_retailer__dcount,
                u107_active_users_brand_retailer_monthly__dcount_user)                    AS t016__atf__monthly_retailer
    FROM joins)

SELECT *
FROM aggs