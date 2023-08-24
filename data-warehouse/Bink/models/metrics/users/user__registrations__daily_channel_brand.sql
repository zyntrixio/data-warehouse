/*
Created by:         Christopher Mitchell
Created date:       2023-05-23
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    cumulative and period metrics on user registration and deregistration broken down by channel and brand on a daily basis
Parameters:
    source_object       - user_trans
                        - stg_metrics__dim_date

*/
with
fact_usr as (select * from {{ ref("users_trans") }}),

dim_date as (
    select *
    from {{ ref("stg_metrics__dim_date") }}
    where
        date >= (select min(date(from_date)) from fact_usr)
        and date <= current_date()
),

usr_staging as (
    select
        d.date,
        u.channel,
        u.brand,
        coalesce(
            count(case when event_type = 'CREATED' then 1 end), 0
        ) as daily_registrations,  -- WHEN CREATE EVENT
        coalesce(
            count(case when event_type = 'DELETED' then 1 end), 0
        ) as daily_deregistrations  -- WHEN DELETE EVENT
    from fact_usr u
    left join dim_date d on d.date = date(u.from_date)
    group by d.date, u.channel, u.brand
),

usr_staging_snap as (
    select
        d.date,
        u.channel,
        u.brand,
        coalesce(
            count(case when event_type = 'CREATED' then 1 end), 0
        ) as snap_user_registrations,
        coalesce(
            count(case when event_type = 'DELETED' then 1 end), 0
        ) as snap_user_deregistrations
    from fact_usr u
    left join
        dim_date d
        on d.date >= date(from_date) and d.date < date(u.to_date)
    group by d.date, u.channel, u.brand
),

combine_all as (
    select
        coalesce(a.date, s.date) as date,
        coalesce(a.channel, s.channel) as channel,
        coalesce(a.brand, s.brand) as brand,
        -- AUTH_TYPES
        coalesce(a.daily_registrations, 0) as daily_registrations,
        coalesce(a.daily_deregistrations, 0) as daily_deregistrations,
        coalesce(s.snap_user_registrations, 0) as snap_user_registrations,
        coalesce(s.snap_user_deregistrations, 0) as snap_user_deregistrations
    from usr_staging a
    full outer join usr_staging_snap s on a.date = s.date and a.brand = s.brand
),

rename as (
    select
        date,
        channel,
        brand,
        daily_registrations
            as u005__registered_users__daily_channel_brand__count,
        daily_deregistrations
            as u006__deregistered_users__daily_channel_brand__count,
        snap_user_registrations
            as u001__registered_users__daily_channel_brand__pit,
        snap_user_deregistrations
            as u002__deregistered_users__daily_channel_brand__pit
    from combine_all
)

select *
from rename
