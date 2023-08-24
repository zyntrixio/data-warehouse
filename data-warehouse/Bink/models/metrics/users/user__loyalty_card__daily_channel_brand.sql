/*
Created by:         Christopher Mitchell
Created date:       2023-06-07
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    Users with Linked Loyalty Card broken down daily, by channel and brand
Parameters:
    source_object       - lc_trans
                        - stg_metrics__dim_date
*/
with
lc_events as (select * from {{ ref("lc_trans") }}),

dim_date as (
    select *
    from {{ ref("stg_metrics__dim_date") }}
    where
        date >= (select min(from_date) from lc_events)
        and date <= current_date()
),

count_up_snap as (
    select
        d.date,
        u.channel,
        u.brand,
        -- Links and Joins
        coalesce(
            count(distinct case when event_type = 'SUCCESS' then user_ref end),
            0
        ) as u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit
    from lc_events u
    left join
        dim_date d
        on
            d.date >= date(u.from_date)
            and d.date < coalesce(date(u.to_date), '9999-12-31')
    group by d.date, u.brand, u.channel
    having date is not null
)

select *
from count_up_snap
