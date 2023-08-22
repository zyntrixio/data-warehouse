/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the base table for scheme_schemeaccount this holds information about loyalty cards

Parameters:
    source_object      - Hermes.SCHEME_SCHEMEACCOUNT
*/
with
    source as (select * from {{ source("Hermes", "SCHEME_SCHEMEACCOUNT") }}),
    renaming as (

        select
            balances,
            id as loyalty_card_id,
            link_date::timestamp as link_date,
            scheme_id as loyalty_plan_id,
            _airbyte_ab_id,
            _airbyte_scheme_schemeaccount_hashid,
            join_date::timestamp as join_date,
            card_number,
            updated::timestamp as updated,
            barcode,
            vouchers,
            created::timestamp as created,
            "order" as orders,
            transactions,
            originating_journey,
            pll_links,
            _airbyte_emitted_at,
            formatted_images,
            is_deleted,
            _airbyte_normalized_at
        from source

    )

select *
from renaming
