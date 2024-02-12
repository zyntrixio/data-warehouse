/*
Test to ensure no create user events are followed by another create user event

Created By:     SP
Created Date:   2022/07/12
*/
{{
    config(
        tags=["business"],
        meta={
            "description": "test to ensure no create user events are followed by another create user event in last 24 hours.",
            "test_type": "Business",
        },
    )
}}

with
    all_events as (
        select
            user_id,
            "EVENT_TYPE",
            lead("EVENT_TYPE") over (
                partition by user_id order by event_date_time, event_id
            ) as next_event
        from {{ ref("fact_user") }}
        where
            "EVENT_TYPE" = 'CREATED'
            and timediff(
                hour,
                event_date_time,
                (select max(event_date_time) from {{ ref("fact_user") }})
            )
            < 24
    ),
    consecutive_creates as (select * from all_events where next_event = 'CREATED')

select *
from consecutive_creates
