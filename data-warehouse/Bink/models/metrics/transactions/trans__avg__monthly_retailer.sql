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
with
    trans_events as (select * from {{ ref("trans__trans__monthly_retailer") }}),
    user_events as (select * from {{ ref("user__transactions__monthly_retailer") }}),
    joins as (
        select
            t.date,
            t.loyalty_plan_company,
            t.loyalty_plan_name,
            t.t009__spend__monthly_retailer__sum,
            t.t010__refund__monthly_retailer__sum,
            t.t011__txns__monthly_retailer__dcount,
            t.t012__refund__monthly_retailer__dcount,
            t.t013__bnpl_txns__monthly_retailer__dcount,
            u.u107_active_users_brand_retailer_monthly__dcount_uid
        from trans_events t
        left join
            user_events u
            on u.loyalty_plan_company = t.loyalty_plan_company
            and u.date = t.date
    ),
    aggs as (
        select
            date,
            loyalty_plan_company,
            loyalty_plan_name,
            div0(
                t009__spend__monthly_retailer__sum, t011__txns__monthly_retailer__dcount
            ) as t014__aov__monthly_retailer__avg,
            div0(
                t009__spend__monthly_retailer__sum,
                u107_active_users_brand_retailer_monthly__dcount_uid
            ) as t015__arpu__monthly_retailer__avg,
            div0(
                t011__txns__monthly_retailer__dcount,
                u107_active_users_brand_retailer_monthly__dcount_uid
            ) as t016__atf__monthly_retailer__avg
        from joins
    )

select *
from aggs
