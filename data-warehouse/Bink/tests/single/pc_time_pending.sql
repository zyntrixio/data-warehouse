/*
 Test to monitor long delays whilst the payment account is in pending
 
 Created By:     SP
 Created Date:   2022/07/19
*/
{{
    config(
        tags=["business"],
        error_if=">100",
        warn_if=">100",
        meta={
            "description": "Test to monitor long delays (10 mins) whilst the payment account is in pending with set limits.",
            "test_type": "Business",
        },
    )
}}

with
    new_pa as (
        select *
        from {{ ref("fact_payment_account") }}
        where
            event_type = 'ADDED'
            and timediff(
                hour,
                event_date_time,
                (select max(event_date_time) from {{ ref("fact_payment_account") }})
            )
            < 24
    ),
    wait_times as (
        select
            pasc.event_date_time as pending_dt,
            pa.event_date_time as created_dt,
            timediff(minute, created_dt, pending_dt) as wait_minutes_pending
        from new_pa pa
        left join
            {{ ref("fact_payment_account_status_change") }} pasc
            on pa.payment_account_id = pasc.payment_account_id
    )

select *
from wait_times
where wait_minutes_pending > 10
