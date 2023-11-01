/*
Created by:         Christopher Mitchell
Created date:       2023-07-17
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-23

Description:
    Transaction metrics by retailer on a monthly granularity. 
Notes:
    source_object       - txns_trans
                        - stg_metrics__dim_date
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

reward_rules as (
    select * from {{ref("src__retailer_lookups_reward_rules") }}
),

stage as (
    select
        t.user_ref,
        t.transaction_id,
        t.loyalty_plan_name,
        t.loyalty_plan_company,
        t.status,
        date(t.date) as date,
        t.spend_amount,
        t.loyalty_card_id
    from txn_events t
    inner join reward_rules r on r.loyalty_plan_company = t.loyalty_plan_company and r.value <= t.spend_amount
),

-- Slim chickens counts concat of date and user to limit rewards to 1 per day
txn_period as (
    select
        d.start_of_month as date,
        s.loyalty_plan_company,
        s.loyalty_plan_name,
        count(distinct case loyalty_plan_company when 'Slim Chickens' then user_ref||date --count date and user to limit to 1 per day
                else transaction_id -- all other merchants count full txn list
                end) as stamps_issued
    from stage s
    left join dim_date d on d.start_of_month = date_trunc('month', s.date)
    group by d.start_of_month, s.loyalty_plan_company, s.loyalty_plan_name
),

txn_cumulative as (
    select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        sum(stamps_issued) over (
            partition by loyalty_plan_company order by date
        ) as cumulative_stamps_issued
    from txn_period
),

combine_all as (
    select
        coalesce(s.date, p.date) as date,
        coalesce(
            s.loyalty_plan_company, p.loyalty_plan_company
        ) as loyalty_plan_company,
        coalesce(s.loyalty_plan_name, p.loyalty_plan_name) as loyalty_plan_name,
        coalesce(p.stamps_issued, 0) as v013__stamps_issued__monthly_retailer__dcount,
        coalesce(s.cumulative_stamps_issued, 0)
            as v014__stamps_issued__monthly_retailer__cdcount
    from txn_cumulative s
    full outer join
        txn_period p
        on
            s.date = p.date
            and s.loyalty_plan_company = p.loyalty_plan_company
)


select *
from combine_all
