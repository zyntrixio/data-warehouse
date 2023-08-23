/*
Created by:         Christopher Mitchell
Created date:       2023-07-03
Last modified by:
Last modified date:

Description:
    todo
Parameters:
    source_object       - pll_status_trans
*/
with
pll_events as (select * from {{ ref("pll_status_trans") }}),

dim_date as (
    select distinct
        start_of_month,
        end_of_month
    from {{ ref("dim_date") }}
    where
        date >= (select min(from_date) from pll_events)
        and date <= current_date()
),

count_up_snap as (
    select
        d.start_of_month as date,
        u.loyalty_plan_name,
        u.loyalty_plan_company,
        coalesce(
            count(distinct case when active_link then loyalty_card_id end), 0
        ) as pll_active_link_count
    from pll_events u
    left join
        dim_date d
        on
            d.end_of_month >= date(u.from_date)
            and d.end_of_month < coalesce(date(u.to_date), '9999-12-31')
    group by d.start_of_month, u.loyalty_plan_name, u.loyalty_plan_company
    having date is not null
),

rename as (
    select
        date,
        loyalty_plan_name,
        loyalty_plan_company,
        pll_active_link_count
            as lc201__loyalty_card_active_pll__monthly_retailer__pit
    from count_up_snap
)

select *
from rename
