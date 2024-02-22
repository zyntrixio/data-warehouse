/*
Created by:         Christopher Mitchell
Created date:       2023-07-17
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-23

Description:
    Transaction metrics by retailer on a daily granularity. 
Notes:
    source_object       - txns_trans
*/

{{
    config(
        materialized="incremental"
    )
}}

with
txn_events as (select * from {{ ref("txns_trans") }}
    {% if is_incremental() %}
            where
            inserted_date_time >= (select max(inserted_date_time) from {{ this }})
    {% endif %}
),

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

txn_union as (
    select * from txn_period
    {% if is_incremental() %}
    union
    select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        t033__spend__daily_retailer__sum,
        t034__refund__daily_retailer__sum,
        t039__net_spend__daily_retailer__sum,
        t038__bnpl_txns__daily_retailer__dcount,
        t035__txns__daily_retailer__dcount,
        t036__refund__daily_retailer__dcount,
        t037__duplicate_txn__daily_retailer__dcount
    from {{ this }}
    {% endif %}
),

txn_combine as (
    select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        sum(spend_amount_period_positive) as spend_amount_period_positive,
        sum(refund_amount_period) as refund_amount_period,
        sum(net_spend_amount_period) as net_spend_amount_period,
        sum(count_bnpl_period) as count_bnpl_period,
        sum(count_transaction_period) as count_transaction_period,
        sum(count_refund_period) as count_refund_period,
        sum(count_dupe_period) as count_dupe_period
    from txn_union
    group by date, loyalty_plan_company, loyalty_plan_name
),

txn_cumulative as (
    select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        spend_amount_period_positive,
        refund_amount_period,
        net_spend_amount_period,
        count_bnpl_period,
        count_transaction_period,
        count_refund_period,
        count_dupe_period,
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
    from txn_combine
),

finalise as 
    (select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        cumulative_spend as t027__spend__daily_retailer__csum,
        cumulative_refund as t028__refund__daily_retailer__csum,
        cumulative_txns as t029__txns__daily_retailer__csum,
        cumulative_refund_txns as t030__refund__daily_retailer__csum,
        cumulative_dupe_txns as t031__duplicate_txn__daily_retailer__csum,
        cumulative_bnpl_txns as t032__bnpl_txns__daily_retailer__csum,
        spend_amount_period_positive as t033__spend__daily_retailer__sum,
        refund_amount_period as t034__refund__daily_retailer__sum,
        count_transaction_period as t035__txns__daily_retailer__dcount,
        count_refund_period as t036__refund__daily_retailer__dcount,
        count_bnpl_period as t038__bnpl_txns__daily_retailer__dcount,
        count_dupe_period as t037__duplicate_txn__daily_retailer__dcount,
        net_spend_amount_period as t039__net_spend__daily_retailer__sum,
        cumulative_net_spend as t040__net_spend__daily_retailer__csum,
        t035__txns__daily_retailer__dcount+t036__refund__daily_retailer__dcount as t041__txns_and_refunds__daily_retailer__dcount,
        t037__duplicate_txn__daily_retailer__dcount+t035__txns__daily_retailer__dcount as t042__txns_and_dupes__daily_retailer__dcount,
        DIV0(t037__duplicate_txn__daily_retailer__dcount,t042__txns_and_dupes__daily_retailer__dcount) as t043__duplicate_txn_per_txn__daily_retailer__percentage,
        sysdate() as inserted_date_time
    from txn_cumulative
)


select *
from finalise
