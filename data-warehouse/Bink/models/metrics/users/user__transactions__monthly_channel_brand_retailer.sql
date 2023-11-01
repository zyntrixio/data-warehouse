/*
Created by:         Christopher Mitchell
Created date:       2023-07-03
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    User transaction metrics monthly by channel, brand and retailera
Parameters:
    source_object       - txns_trans
*/
with
user_events as (select * from {{ ref("txns_trans") }}),

dim_date as (
    select distinct
        start_of_month,
        end_of_month
    from {{ ref("stg_metrics__dim_date") }}
    where date >= (select min(date) from user_events) and date <= current_date()
),

user_snap as (
    select
        d.start_of_month as date,
        u.channel,
        u.brand,
        u.loyalty_plan_company,
        u.loyalty_plan_name,
        coalesce(count(
            distinct case when status = 'TXNS' then user_ref end
        ), 0) as u110__active_users__monthly_channel_brand_retailer__cdcount_uid
    from user_events u
    left join dim_date d on date(u.date) <= d.end_of_month
    group by d.start_of_month, u.channel, u.brand, u.loyalty_plan_company, u.loyalty_plan_name
),

user_period as (
    select
        d.start_of_month as date,
        u.channel,
        u.brand,
        u.loyalty_plan_company,
        u.loyalty_plan_name,
        coalesce(
            count(distinct case when status = 'TXNS' then user_ref end), 0
        ) as u109__active_users__monthly_channel_brand_retailer__dcount_uid
    from user_events u
    left join dim_date d on d.start_of_month = date_trunc('month', u.date)
    group by d.start_of_month, u.channel, u.brand, u.loyalty_plan_company, u.loyalty_plan_name
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
        coalesce(
            p.u109__active_users__monthly_channel_brand_retailer__dcount_uid, 0
        ) as u109__active_users__monthly_channel_brand_retailer__dcount_uid,
        coalesce(
            s.u110__active_users__monthly_channel_brand_retailer__cdcount_uid, 0
        ) as u110__active_users__monthly_channel_brand_retailer__cdcount_uid
    from user_snap s
    full outer join
        user_period p
        on
            s.date = p.date
            and s.loyalty_plan_company = p.loyalty_plan_company
            and s.brand = p.brand
)

select *
from combine_all
