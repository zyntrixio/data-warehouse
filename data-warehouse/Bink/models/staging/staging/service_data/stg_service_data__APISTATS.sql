/*
Created by:         Anand Bhakta
Created date:       2023-02-02
Last modified by:
Last modified date:

Description:
    Stages the apistats table

Parameters:
    source_object      - SNOWSTORM.APISTATS
*/
{{ config(materialized="incremental", unique_key="API_ID") }}

with
all_events as (
    select *
    from {{ source("snowstorm", "apistats") }}
    {% if is_incremental() %}
        where
            _airbyte_emitted_at
            >= (select max(_airbyte_emitted_at) from {{ this }})
    {% endif %}

),

all_events_select as (
    select
        id as api_id,
        path,
        method,
        -- ms_pop,
        client_ip,
        date_time,
        user_agent,
        status_code,
        response_time,
        -- client_country,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_apistats_hashid
    from all_events
)

select *
from all_events_select
