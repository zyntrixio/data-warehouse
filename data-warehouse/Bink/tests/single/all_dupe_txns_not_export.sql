/*
 Test to ensure all duplicate transactions are not visable in the exported transactions
 
 Created By:     Chris Mitchell
 Created Date:   2023-08-09

 */
{{
    config(
        tags=["business"],
        error_if=">0",
        meta={
            "description": "Test to ensure all duplicate transactions are not visable in the exported transactions",
            "test_type": "Business",
        },
    )
}}

with
    dupes as (
        select *
        from {{ ref("fact_transaction_secure") }}
        where duplicate_transaction = true
    ),
    exports as (
        select *
        from {{ ref("fact_transaction_secure") }}
        where duplicate_transaction = false
    ),
    test_outputs as (
        select d.transaction_id
        from dupes d
        intersect
        select e.transaction_id
        from exports e
    )

select *
from test_outputs
