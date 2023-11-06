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
        u.U200_ACTIVE_USERS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID,
        t.t020__net_spend__monthly_retailer__sum,
        t.T065__TXNS_AND_REFUNDS__MONTHLY_RETAILER_CHANNEL__DCOUNT,
        u.U203_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID
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
        ) as T054__AOV__MONTHLY_RETAILER_CHANNEL__AVG,
        div0(
            t009__spend__monthly_retailer__sum,
            U200_ACTIVE_USERS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID
        ) as T055__ARPU__MONTHLY_RETAILER_CHANNEL__AVG,
        div0(
            t011__txns__monthly_retailer__dcount,
            U200_ACTIVE_USERS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID
        ) as T056__ATF__MONTHLY_RETAILER_CHANNEL__AVG,
        div0(
            t020__net_spend__monthly_retailer__sum,
            T065__TXNS_AND_REFUNDS__MONTHLY_RETAILER_CHANNEL__DCOUNT
        ) as T062__AOV_INC_REFUNDS__MONTHLY_RETAILER_CHANNEL__AVG,
        div0(
            t020__net_spend__monthly_retailer__sum,
            U203_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID
        ) as T063__ARPU_INC_REFUNDS__MONTHLY_RETAILER_CHANNEL__AVG,
        div0(
            T065__TXNS_AND_REFUNDS__MONTHLY_RETAILER_CHANNEL__DCOUNT,
            U203_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID
        ) as T064__ATF_INC_REFUNDS__MONTHLY_RETAILER_CHANNEL__AVG
    from joins
)

select *
from aggs
