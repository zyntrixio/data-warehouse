/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-01
Last modified by:   Anand Bhakta
Last modified date: 2023-12-18

DESCRIPTION:
    TRANSACTION AVERAGE METRICS SUCH AS AVERAGE REVENUE PER USERS, PER MONTH BY RETAILER. 
NOTES:
    SOURCE_OBJECT       - TRANS__TRANS__monthly_channel_brand_retailer
                        - USER__TRANSACTIONS__monthly_channel_brand_retailer
*/

with
trans_events as (select * from {{ ref("trans__trans__monthly_channel_brand_retailer") }}),

user_events as (select * from {{ ref("user__transactions__monthly_channel_brand_retailer") }}
),

joins as (
    select
        t.date,
        t.channel,
        t.loyalty_plan_company,
        t.loyalty_plan_name,
        sum(t.t049__spend__monthly_channel_brand_retailer__sum) as t049__spend__monthly_channel_brand_retailer__sum,
        sum(t.t050__refund__monthly_channel_brand_retailer__sum) as t050__refund__monthly_channel_brand_retailer__sum,
        sum(t.t051__txns__monthly_channel_brand_retailer__dcount) as t051__txns__monthly_channel_brand_retailer__dcount,
        sum(t.t052__refund__monthly_channel_brand_retailer__dcount) as t052__refund__monthly_channel_brand_retailer__dcount,
        sum(t.t053__bnpl_txns__monthly_channel_brand_retailer__dcount) as t053__bnpl_txns__monthly_channel_brand_retailer__dcount,
        sum(u.u200_active_users__monthly_channel_brand_retailer__dcount_uid) as u200_active_users__monthly_channel_brand_retailer__dcount_uid,
        sum(t.t060__net_spend__monthly_channel_brand_retailer__sum) as t060__net_spend__monthly_channel_brand_retailer__sum,
        sum(t.t065__txns_and_refunds__monthly_channel_brand_retailer__dcount) as t065__txns_and_refunds__monthly_channel_brand_retailer__dcount,
        sum(u.u203_active_users_inc_refunds__monthly_channel_brand_retailer__dcount_uid) as u203_active_users_inc_refunds__monthly_channel_brand_retailer__dcount_uid
    from trans_events t
    left join
        user_events u
        on
            u.loyalty_plan_company = t.loyalty_plan_company
            and u.channel = t.channel
            and u.date = t.date
    group by
        t.date,
        t.channel,
        t.loyalty_plan_company,
        t.loyalty_plan_name
),

aggs as (
    select
        date,
        channel,
        loyalty_plan_company,
        loyalty_plan_name,
        div0(
            t049__spend__monthly_channel_brand_retailer__sum,
            t051__txns__monthly_channel_brand_retailer__dcount
        ) as t054__aov__monthly_channel_brand_retailer__avg,
        div0(
            t049__spend__monthly_channel_brand_retailer__sum,
            u200_active_users__monthly_channel_brand_retailer__dcount_uid
        ) as t055__arpu__monthly_channel_brand_retailer__avg,
        div0(
            t051__txns__monthly_channel_brand_retailer__dcount,
            u200_active_users__monthly_channel_brand_retailer__dcount_uid
        ) as t056__atf__monthly_channel_brand_retailer__avg,
        div0(
            t060__net_spend__monthly_channel_brand_retailer__sum,
            t065__txns_and_refunds__monthly_channel_brand_retailer__dcount
        ) as t062__aov_inc_refunds__monthly_channel_brand_retailer__avg,
        div0(
            t060__net_spend__monthly_channel_brand_retailer__sum,
            u203_active_users_inc_refunds__monthly_channel_brand_retailer__dcount_uid
        ) as t063__arpu_inc_refunds__monthly_channel_brand_retailer__avg,
        div0(
            t065__txns_and_refunds__monthly_channel_brand_retailer__dcount,
            u203_active_users_inc_refunds__monthly_channel_brand_retailer__dcount_uid
        ) as t064__atf_inc_refunds__monthly_channel_brand_retailer__avg
    from joins
)

select *
from aggs
