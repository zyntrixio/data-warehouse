/*
Created by:         Anand Bhakta
Created date:       2023-02-02
Last modified by:   
Last modified date: 

Description:
    Stages the freshservice table

Parameters:
    source_object      - SNOWSTORM.FRESHSERVICE
*/
with
    all_data as (select * from {{ source("snowstorm", "freshservice") }}),
    all_data_select as (
        select
            {{ dbt_utils.surrogate_key(["ID", "UPDATED_AT"]) }} as id,
            id as ticket_id,
            mi,
            status,
            channel,
            service,
            created_at,
            updated_at,
            sla_breached,
            _airbyte_emitted_at
        from all_data
    )

select *
from all_data_select
