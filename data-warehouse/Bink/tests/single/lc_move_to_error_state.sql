/*
 Test to ensure all lc are not ending with error states
 
 Created By:     SP
 Created Date:   2022/07/1
 */
{{
    config(
        tags=["business"],
        error_if=">100",
        warn_if=">100",
        meta={
            "description": "Test to ensure all lc within the last day are not ending in error states with set limits.",
            "test_type": "Business",
        },
    )
}}


with
    lc_errors as (
        select loyalty_card_id
        from {{ ref("fact_loyalty_card_status_change") }}
        where
            is_most_recent = true
            and to_status_id not in (0, 1)
            and timediff(
                hour,
                event_date_time,
                (
                    select max(event_date_time)
                    from {{ ref("fact_loyalty_card_status_change") }}
                )
            )
            < 24
    ),
    previously_valid as (
        select loyalty_card_id
        from {{ ref("fact_loyalty_card_status_change") }}
        where
            loyalty_card_id in (select loyalty_card_id from lc_errors)
            and to_status_id in (0, 1)
        group by loyalty_card_id

    )

select *
from previously_valid
