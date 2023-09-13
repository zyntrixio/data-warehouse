/*
Created by:         Christopher Mitchell
Created date:       2023-07-17
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-23

Description:
    Rewrite of metrics for transactions, monthly by retailer
Notes:
    source_object       - txns_trans
*/
with
txn_events as (select * from {{ ref("txns_trans") }}),

dim_date as (
    select distinct
        date
    from {{ ref("stg_metrics__dim_date") }}
    where date >= (select min(date) from txn_events) and date <= current_date()
),

stage as (
    select
        user_ref,
        transaction_id,
        loyalty_plan_name,
        loyalty_plan_company,
        status,
        DATE(date) as date,
        spend_amount,
        loyalty_card_id
    from txn_events
),

txn_period as (
    select
        d.date as date,
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
    left join dim_date d on d.date = s.date
    group by d.date, s.loyalty_plan_company, s.loyalty_plan_name
),

txn_cumulative as (
    select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        sum(spend_amount_period_positive) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_spend,
        sum(refund_amount_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_refund,
        sum(net_spend_amount_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_net_spend,
        sum(count_bnpl_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_bnpl_txns,
        sum(count_transaction_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_txns,
        sum(count_refund_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_refund_txns,
        sum(count_dupe_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_dupe_txns
    from txn_period
),

combine_all as (
    select
        coalesce(s.date, p.date) as date,
        coalesce(
            s.loyalty_plan_company, p.loyalty_plan_company
        ) as loyalty_plan_company,
        coalesce(s.loyalty_plan_name, p.loyalty_plan_name) as loyalty_plan_name,
        coalesce(s.cumulative_spend, 0) as t027__spend__daily_retailer__csum,
        coalesce(s.cumulative_refund, 0)
            as t028__refund__daily_retailer__csum,
        coalesce(s.cumulative_txns, 0) as t029__txns__daily_retailer__csum,
        coalesce(
            s.cumulative_refund_txns, 0
        ) as t030__refund__daily_retailer__csum,
        coalesce(
            s.cumulative_dupe_txns, 0
        ) as t031__duplicate_txn__daily_retailer__csum,
        coalesce(
            s.cumulative_bnpl_txns, 0
        ) as t032__bnpl_txns__daily_retailer__csum,
        coalesce(
            p.spend_amount_period_positive, 0
        ) as t033__spend__daily_retailer__sum,
        coalesce(p.refund_amount_period, 0)
            as t034__refund__daily_retailer__sum,
        coalesce(
            p.count_transaction_period, 0
        ) as t035__txns__daily_retailer__dcount,
        coalesce(p.count_refund_period, 0)
            as t036__refund__daily_retailer__dcount,
        coalesce(
            p.count_bnpl_period, 0
        ) as t038__bnpl_txns__daily_retailer__dcount,
        coalesce(p.count_dupe_period, 0)
            as t037__duplicate_txn__daily_retailer__dcount,
        coalesce(p.net_spend_amount_period, 0)
            as t039__net_spend__daily_retailer__sum,
        coalesce(p.net_spend_amount_period, 0)
            as t040__net_spend__daily_retailer__csum
    from txn_cumulative s
    full outer join
        txn_period p
        on
            s.date = p.date
            and s.loyalty_plan_company = p.loyalty_plan_company
),

finalise as 
    (select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        t027__spend__daily_retailer__csum,
        t028__refund__daily_retailer__csum,
        t029__txns__daily_retailer__csum,
        t030__refund__daily_retailer__csum,
        t031__duplicate_txn__daily_retailer__csum,
        t032__bnpl_txns__daily_retailer__csum,
        t033__spend__daily_retailer__sum,
        t034__refund__daily_retailer__sum,
        t035__txns__daily_retailer__dcount,
        t036__refund__daily_retailer__dcount,
        t037__duplicate_txn__daily_retailer__dcount,
        t038__bnpl_txns__daily_retailer__dcount,
        t039__net_spend__daily_retailer__sum,
        t040__net_spend__daily_retailer__csum,
        t035__txns__daily_retailer__dcount+t036__refund__daily_retailer__dcount as t041__txns_and_refunds__daily_retailer__dcount,
        t037__duplicate_txn__daily_retailer__dcount+t035__txns__daily_retailer__dcount as t042__txns_and_dupes__daily_retailer__dcount,
        DIV0(t037__duplicate_txn__daily_retailer__dcount,t042__txns_and_dupes__daily_retailer__dcount) as t043__duplicate_txn_per_txn__daily_retailer__percentage

    from combine_all
)


select *
from finalise
