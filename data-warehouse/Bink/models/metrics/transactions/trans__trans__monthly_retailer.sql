/*
Created by:         Christopher Mitchell
Created date:       2023-07-17
Last modified by:
Last modified date:

Description:
    Rewrite of metrics for transactions at a monthly agg
Notes:
    source_object       - fact transaction
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
        s.loyalty_plan_company,
        s.loyalty_plan_name,
        sum(
            case when status = 'TXNS' then s.spend_amount end
        ) as spend_amount_period_positive,
        sum(
            case when status = 'REFUND' then s.spend_amount end
        ) as refund_amount_period,
        count(
            distinct case when status = 'BNPL' then transaction_id end
        ) as count_bnpl_period,
        count(
            distinct case when status = 'TXNS' then transaction_id end
        ) as count_transaction_period,
        count(
            distinct case when status = 'REFUND' then transaction_id end
        ) as count_refund_period
    from stage s
    left join dim_date d on d.start_of_month = date_trunc('month', s.date)
    group by d.start_of_month, s.loyalty_plan_company, s.loyalty_plan_name
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
        sum(count_bnpl_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_bnpl_txns,
        sum(count_transaction_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_txns,
        sum(count_refund_period) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_refund_txns
    from txn_period
),

combine_all as (
    select
        coalesce(s.date, p.date) as date,
        coalesce(
            s.loyalty_plan_company, p.loyalty_plan_company
        ) as loyalty_plan_company,
        coalesce(s.loyalty_plan_name, p.loyalty_plan_name) as loyalty_plan_name,
        coalesce(s.cumulative_spend, 0) as t004__spend__monthly_retailer__csum,
        coalesce(s.cumulative_refund, 0)
            as t005__refund__monthly_retailer__csum,
        coalesce(s.cumulative_txns, 0) as t006__txns__monthly_retailer__csum,
        coalesce(
            s.cumulative_refund_txns, 0
        ) as t007__refund__monthly_retailer__csum,
        coalesce(
            s.cumulative_bnpl_txns, 0
        ) as t008__bnpl_txns__monthly_retailer__csum,
        coalesce(
            p.spend_amount_period_positive, 0
        ) as t009__spend__monthly_retailer__sum,
        coalesce(p.refund_amount_period, 0)
            as t010__refund__monthly_retailer__sum,
        coalesce(
            p.count_transaction_period, 0
        ) as t011__txns__monthly_retailer__dcount,
        coalesce(p.count_refund_period, 0)
            as t012__refund__monthly_retailer__dcount
        ,
        coalesce(
            p.count_bnpl_period, 0
        ) as t013__bnpl_txns__monthly_retailer__dcount
    from txn_cumulative s
    full outer join
        txn_period p
        on
            s.date = p.date
            and s.loyalty_plan_company = p.loyalty_plan_company
)

select *
from combine_all
