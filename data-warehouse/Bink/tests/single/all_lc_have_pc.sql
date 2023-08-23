/*
 Test to ensure every active barclays loyalty card is matched with a payment card
 
 Created By:     SP
 Created Date:   2022/07/19
 */
{{
    config(
        tags=["business"],
        error_if=">100",
        warn_if=">100",
        meta={
            "description": "Test to ensure all active Barcalys loyalty cards are linked to a payment card.",
            "test_type": "Business",
        },
    )
}}

with
    new_lc as (
        select *
        from {{ ref("fact_loyalty_card") }}
        where
            auth_type in ('REGISTER', 'JOIN')
            and event_type = 'SUCCESS'
            and channel like '%barclays%'
            and timediff(
                hour,
                event_date_time,
                (select max(event_date_time) from {{ ref("fact_loyalty_card") }})
            )
            < 24
    )

select user_id
from new_lc
where user_id not in (select user_id from {{ ref("fact_payment_account") }})
