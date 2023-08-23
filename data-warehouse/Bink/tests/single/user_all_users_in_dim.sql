/*
Test to ensure all create user events have a matching user in dim_user

Created By:     SP
Created Date:   2022/07/12
*/
{{
    config(
        tags=["business"],
        meta={
            "description": "Test to ensure all create user events have a matching user in dim_user.",
            "test_type": "Business",
        },
    )
}}

with
    new_users as (
        select *
        from {{ ref("fact_user") }}
        where
            event_type = 'CREATED'
            and is_most_recent = true
            and timediff(
                hour,
                event_date_time,
                (select max(event_date_time) from {{ ref("fact_user") }})
            )
            < 24
    )

select *
from new_users
where user_id not in (select user_id from {{ ref("dim_user") }})
