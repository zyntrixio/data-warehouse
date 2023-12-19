/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-01
Last modified by: Anand Bhakta
Last modified date: 2023-12-19

DESCRIPTION:
    TRANSACTION METRICS BY RETAILER ON A MONTHLY GRANULARITY. 
NOTES:
    SOURCE_OBJECT       - TXNS_TRANS
                        - STG_METRICS__DIM_DATE
*/

with
txn_events as (select * from {{ ref("txns_trans") }}),

dim_date as (
    select distinct
        start_of_month,
        end_of_month
    from {{ ref("stg_metrics__dim_date") }}
    where date >= (select min(date) from txn_events) and date <= current_date()
),

stage as (
    select
        user_ref,
        transaction_id,
        channel,
        brand,
        loyalty_plan_name,
        loyalty_plan_company,
        status,
        date_trunc('month', date) as date,
        spend_amount,
        loyalty_card_id
    from txn_events
),

txn_period as (
    select
        d.start_of_month as date,
        s.channel,
        s.brand,
        s.loyalty_plan_company,
        s.loyalty_plan_name,
        sum(
            case when status = 'TXNS' then s.spend_amount end
        ) as spend_amount_period_positive,
        sum(
            case when status = 'REFUND' then s.spend_amount end
        ) as refund_amount_period,
        sum(
            case when status in ('TXNS','REFUND') then s.spend_amount end
        ) as net_spend_amount_period,
        count(
            distinct case when status = 'BNPL' then transaction_id end
        ) as count_bnpl_period,
        count(
            distinct case when status = 'TXNS' then transaction_id end
        ) as count_transaction_period,
        count(
            distinct case when status = 'REFUND' then transaction_id end
        ) as count_refund_period,
        count(
            distinct case when status = 'DUPLICATE' then transaction_id end
        ) as count_dupe_period
    from stage s
    left join dim_date d on d.start_of_month = date_trunc('month', s.date)
    group by d.start_of_month, s.channel, s.brand, s.loyalty_plan_company, s.loyalty_plan_name
),

txn_cumulative as (
    select
        date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        sum(spend_amount_period_positive) over (
            partition by channel, brand, loyalty_plan_company order by date
        ) as cumulative_spend,
        sum(refund_amount_period) over (
            partition by channel, brand, loyalty_plan_company order by date
        ) as cumulative_refund,
        sum(net_spend_amount_period) over (
            partition by channel, brand, loyalty_plan_company order by date
        ) as cumulative_net_spend,
        sum(count_bnpl_period) over (
            partition by channel, brand, loyalty_plan_company order by date
        ) as cumulative_bnpl_txns,
        sum(count_transaction_period) over (
            partition by channel, brand, loyalty_plan_company order by date
        ) as cumulative_txns,
        sum(count_refund_period) over (
            partition by channel, brand, loyalty_plan_company order by date
        ) as cumulative_refund_txns,
        sum(count_dupe_period) over (
            partition by channel, brand, loyalty_plan_company order by date
        ) as cumulative_dupe_txns
    from txn_period
),

combine_all as (
    select
        coalesce(s.date, p.date) as date,
        coalesce(s.channel, p.channel) as channel,
        coalesce(s.brand, p.brand) as brand,
        coalesce(
            s.loyalty_plan_company, p.loyalty_plan_company
        ) as loyalty_plan_company,
        coalesce(s.loyalty_plan_name, p.loyalty_plan_name) as loyalty_plan_name,
        coalesce(s.cumulative_spend, 0) as T044__SPEND__monthly_channel_brand_retailer__CSUM,
        coalesce(s.cumulative_refund, 0)
            as T045__REFUND__monthly_channel_brand_retailer__CSUM,
        coalesce(s.cumulative_txns, 0) as T046__TXNS__MONTHLY_RETAILER_CAHNNEL__CSUM,
        coalesce(
            s.cumulative_refund_txns, 0
        ) as T047__REFUND__monthly_channel_brand_retailer__CSUM,
        coalesce(
            s.cumulative_dupe_txns, 0
        ) as T058__DUPLICATE_TXN__monthly_channel_brand_retailer__CSUM,
        coalesce(
            s.cumulative_bnpl_txns, 0
        ) as T048__BNPL_TXNS__monthly_channel_brand_retailer__CSUM,
        coalesce(
            p.spend_amount_period_positive, 0
        ) as T049__SPEND__monthly_channel_brand_retailer__SUM,
        coalesce(p.refund_amount_period, 0)
            as T050__REFUND__monthly_channel_brand_retailer__SUM,
        coalesce(
            p.count_transaction_period, 0
        ) as T051__TXNS__monthly_channel_brand_retailer__DCOUNT,
        coalesce(p.count_refund_period, 0)
            as T052__REFUND__monthly_channel_brand_retailer__DCOUNT,
        coalesce(
            p.count_bnpl_period, 0
        ) as T053__BNPL_TXNS__monthly_channel_brand_retailer__DCOUNT,
        coalesce(p.count_dupe_period, 0)
            as T057__DUPLICATE_TXN__monthly_channel_brand_retailer__DCOUNT,
        coalesce(p.net_spend_amount_period, 0)
            as T060__NET_SPEND__monthly_channel_brand_retailer__SUM,
        coalesce(p.net_spend_amount_period, 0)
            as T061__NET_SPEND__monthly_channel_brand_retailer__CSUM
    from txn_cumulative s
    full outer join
        txn_period p
        on
            s.date = p.date
            and s.loyalty_plan_company = p.loyalty_plan_company
            and s.channel = p.channel
            and s.brand = p.brand
),

finalise as 
    (select
        date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        T044__SPEND__monthly_channel_brand_retailer__CSUM,
        T045__REFUND__monthly_channel_brand_retailer__CSUM,
        T046__TXNS__MONTHLY_RETAILER_CAHNNEL__CSUM,
        T047__REFUND__monthly_channel_brand_retailer__CSUM,
        T058__DUPLICATE_TXN__monthly_channel_brand_retailer__CSUM,
        T048__BNPL_TXNS__monthly_channel_brand_retailer__CSUM,
        T049__SPEND__monthly_channel_brand_retailer__SUM,
        T050__REFUND__monthly_channel_brand_retailer__SUM,
        T051__TXNS__monthly_channel_brand_retailer__DCOUNT,
        T052__REFUND__monthly_channel_brand_retailer__DCOUNT,
        T057__DUPLICATE_TXN__monthly_channel_brand_retailer__DCOUNT,
        T053__BNPL_TXNS__monthly_channel_brand_retailer__DCOUNT,
        T060__NET_SPEND__monthly_channel_brand_retailer__SUM,
        T061__NET_SPEND__monthly_channel_brand_retailer__CSUM,
        T051__TXNS__monthly_channel_brand_retailer__DCOUNT+T052__REFUND__monthly_channel_brand_retailer__DCOUNT as t065__txns_and_refunds__monthly_channel_brand_retailer__dcount,
        T057__DUPLICATE_TXN__monthly_channel_brand_retailer__DCOUNT+T051__TXNS__monthly_channel_brand_retailer__DCOUNT as T066__TXNS_AND_DUPES__monthly_channel_brand_retailer__DCOUNT,
        DIV0(T057__DUPLICATE_TXN__monthly_channel_brand_retailer__DCOUNT,T066__TXNS_AND_DUPES__monthly_channel_brand_retailer__DCOUNT) as T059__DUPLICATE_TXN_PER_TXN__monthly_channel_brand_retailer__PERCENTAGE

    from combine_all
)


select *
from finalise
