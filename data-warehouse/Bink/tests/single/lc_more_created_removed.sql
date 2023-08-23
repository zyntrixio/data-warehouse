/*
 This test ensures there are not more deleted events than created
 
 Created By:     SP
 Created Date:   2022/07/13
 */
{{
    config(
        tags=["business"],
        meta={
            "description": "This test ensures there are not more deleted events than created",
            "test_type": "Business",
        },
        enabled=False,
    )
}}

with
    joins as (
        select *
        from {{ ref("fact_loyalty_card_join") }}
        where event_type = 'SUCCESS' and is_most_recent = true
    ),

    removals as (
        select r.loyalty_card_id
        from {{ ref("fact_loyalty_card_removed") }} r
        left join joins j on j.loyalty_card_id = r.loyalty_card_id
    )

select loyalty_card_id
from {{ ref("fact_loyalty_card_removed") }}
minus
select loyalty_card_id
from removals
