/*
 Test to ensure no duplicate fingerprints for a payment account
 
 Created By:     SP
 Created Date:   2022/07/19
 */
{{
    config(
        tags=["business"],
        meta={
            "description": "Test to ensure no duplicate fingerprints for a payment accounts created in the last day.",
            "test_type": "Business",
        },
        enabled=False,
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
    fingerprints as (
        select
            pa.user_id,
            count(distinct dpa.fingerprint) as distinct_fingerprints,
            count(dpa.fingerprint) as fingerprints
        from new_pa pa
        left join
            {{ ref("dim_payment_account_secure") }} dpa
            on pa.payment_account_id = dpa.payment_account_id
        group by user_id
    )

select *
from fingerprints
where distinct_fingerprints != fingerprints
