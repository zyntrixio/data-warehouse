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
        t.loyalty_plan_company,
        t.loyalty_plan_name,
        t.t049__spend__monthly_retailer_channel__sum,
        t.t050__refund__monthly_retailer_channel__sum,
        t.t051__txns__monthly_retailer_channel__dcount,
        t.t052__refund__monthly_retailer_channel__dcount,
        t.t053__bnpl_txns__monthly_retailer_channel__dcount,
        u.u200_active_users__retailer_monthly_channel__dcount_uid,
        t.t060__net_spend__monthly_retailer_channel__sum,
        t.t065__txns_and_refunds__monthly_retailer_channel__dcount,
        u.u203_active_users_inc_refunds__retailer_monthly_channel__dcount_uid
    from trans_events t
    left join
        user_events u
        on
            u.loyalty_plan_company = t.loyalty_plan_company
            and u.channel = t.channel
            and u.date = t.date
),

aggs as (
    select
        date,
        channel,
        loyalty_plan_company,
        loyalty_plan_name,
        div0(
            t049__spend__monthly_retailer_channel__sum,
            t051__txns__monthly_retailer_channel__dcount
        ) as t054__aov__monthly_retailer_channel__avg,
        div0(
            t049__spend__monthly_retailer_channel__sum,
            u200_active_users__retailer_monthly_channel__dcount_uid
        ) as t055__arpu__monthly_retailer_channel__avg,
        div0(
            t051__txns__monthly_retailer_channel__dcount,
            u200_active_users__retailer_monthly_channel__dcount_uid
        ) as t056__atf__monthly_retailer_channel__avg,
        div0(
            t060__net_spend__monthly_retailer_channel__sum,
            t065__txns_and_refunds__monthly_retailer_channel__dcount
        ) as t062__aov_inc_refunds__monthly_retailer_channel__avg,
        div0(
            t060__net_spend__monthly_retailer_channel__sum,
            u203_active_users_inc_refunds__retailer_monthly_channel__dcount_uid
        ) as t063__arpu_inc_refunds__monthly_retailer_channel__avg,
        div0(
            t065__txns_and_refunds__monthly_retailer_channel__dcount,
            u203_active_users_inc_refunds__retailer_monthly_channel__dcount_uid
        ) as t064__atf_inc_refunds__monthly_retailer_channel__avg
    from joins
)

select *
from aggs
