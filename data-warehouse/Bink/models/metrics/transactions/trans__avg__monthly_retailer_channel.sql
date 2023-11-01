/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-01
LAST MODIFIED BY:   
LAST MODIFIED DATE: 

DESCRIPTION:
    TRANSACTION AVERAGE METRICS SUCH AS AVERAGE REVENUE PER USERS, PER MONTH BY RETAILER. 
NOTES:
    SOURCE_OBJECT       - TRANS__TRANS__MONTHLY_RETAILER_CHANNEL
                        - USER__TRANSACTIONS__MONTHLY_RETAILER_CHANNEL
*/

with
trans_events as (select * from {{ ref("trans__trans__monthly_retailer_channel") }}),

user_events as (select * from {{ ref("user__transactions__monthly_retailer_channel") }}
),

joins as (
    select
        t.date,
        t.channel,
        t.brand,
        t.loyalty_plan_company,
        t.loyalty_plan_name,
        t.t009__spend__monthly_retailer__sum,
        t.t010__refund__monthly_retailer__sum,
        t.t011__txns__monthly_retailer__dcount,
        t.t012__refund__monthly_retailer__dcount,
        t.t013__bnpl_txns__monthly_retailer__dcount,
        u.u107_active_users__retailer_monthly__dcount_uid,
        t.t020__net_spend__monthly_retailer__sum,
        t.t025__txns_and_refunds__monthly_retailer__dcount,
        u.u112_active_users_inc_refunds__retailer_monthly__dcount_uid
    from trans_events t
    left join
        user_events u
        on
            u.loyalty_plan_company = t.loyalty_plan_company
            and u.channel = t.channel
            and u.brand = t.brand
            and u.date = t.date
),

aggs as (
    select
        date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        div0(
            t009__spend__monthly_retailer__sum,
            t011__txns__monthly_retailer__dcount
        ) as t014__aov__monthly_retailer__avg,
        div0(
            t009__spend__monthly_retailer__sum,
            u107_active_users__retailer_monthly__dcount_uid
        ) as t015__arpu__monthly_retailer__avg,
        div0(
            t011__txns__monthly_retailer__dcount,
            u107_active_users__retailer_monthly__dcount_uid
        ) as t016__atf__monthly_retailer__avg,
        div0(
            t020__net_spend__monthly_retailer__sum,
            t025__txns_and_refunds__monthly_retailer__dcount
        ) as t022__aov_inc_refunds__monthly_retailer__avg,
        div0(
            t020__net_spend__monthly_retailer__sum,
            u112_active_users_inc_refunds__retailer_monthly__dcount_uid
        ) as t023__arpu_inc_refunds__monthly_retailer__avg,
        div0(
            t025__txns_and_refunds__monthly_retailer__dcount,
            u112_active_users_inc_refunds__retailer_monthly__dcount_uid
        ) as t024__atf_inc_refunds__monthly_retailer__avg
    from joins
)

select *
from aggs
