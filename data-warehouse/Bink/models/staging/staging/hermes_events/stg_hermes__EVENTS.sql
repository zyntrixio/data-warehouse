/*
Created by:         Sam Pibworth
Created date:       2022-05-04
Last modified by:
Last modified date:

Description:
    Stages the events table, which is an aggregation of all hermes events including user, payment, and loyalty card information

Parameters:
    source_object      - HERMES_EVENTS.EVENTS
*/
{{ config(materialized="incremental", unique_key="EVENT_ID") }}

with
all_events as (
    select *
    -- this is pointing to service_data schema
    from {{ source("snowstorm", "events") }}{{ var("qa_data_suffix")}}
    {% if is_incremental() %}
        where
            _airbyte_emitted_at
            >= (select max(_airbyte_emitted_at) from {{ this }})
    {% endif %}

),

all_events_select as (
    select
        parse_json(json) as json,
        event_type,
        event_date_time::timestamp as event_date_time,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_events_hashid,
        -- ,_AIRBYTE_UNIQUE_KEY
        id as event_id
    from all_events
)

select *
from all_events_select
