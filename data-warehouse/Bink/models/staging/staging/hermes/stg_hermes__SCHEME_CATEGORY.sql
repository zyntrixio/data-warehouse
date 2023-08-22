/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the lookup table of the loyalty plan categorys

Parameters:
    source_object      - Hermes.SCHEME_CATEGORY
*/
with
    source as (select * from {{ source("Hermes", "SCHEME_CATEGORY") }}),
    renaming as (

        select
            _airbyte_normalized_at,
            _airbyte_ab_id,
            name as loyalty_plan_category,
            _airbyte_emitted_at,
            id as loyalty_plan_category_id,
            _airbyte_scheme_category_hashid
        from source

    )

select *
from renaming
