/*
Created by:         Anand Bhakta
Created date:       2022-11-30
Last modified by:
Last modified date:

Description:
    Add brand and alias channel for all events that have a channel field.

Parameters:
    ref_object      - stg_hermes__EVENTS
*/
{{ config(materialized="incremental", unique_key="EVENT_ID") }}

with
new_events as (
    select *
    from {{ ref("stg_hermes__EVENTS") }}
    {% if is_incremental() %}
        where
            _airbyte_emitted_at
            >= (select max(inserted_date_time) from {{ this }})
    {% endif %}
),

extract_channel as (
    select
        json,
        event_type,
        event_date_time,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_events_hashid,
        event_id,
        json:channel::varchar as channel
    from new_events
),

transform_brand_time as (
    select
        json,
        event_type,
        case
            when event_type in ('lc.addandauth.request', 'lc.auth.request')
                -- half second penalty to fix event order issue
                then dateadd('seconds', -0.5, event_date_time)
            else event_date_time
        end as event_date_time,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_events_hashid,
        event_id,
        case
            when
                channel in (
                    'com.bos.api2', 'com.halifax.api2', 'com.lloyds.api2'
                )
                then 'LLOYDS'
            when channel in ('com.barclays.bmb')
                then 'BARCLAYS'
            when channel in ('com.bink.wallet')
                then 'BINK'
            when channel in ('com.stonegate.mixr')
                then 'MiXR'
            else channel
        end as channel,
        case
            when
                channel in (
                    'com.bos.api2', 'com.halifax.api2', 'com.lloyds.api2'
                )
                then upper(split_part(channel, '.', 2))
            when channel in ('com.barclays.bmb')
                then 'BARCLAYS'
            when channel in ('com.bink.wallet')
                then 'BINK'
            when channel in ('com.stonegate.mixr')
                then 'MiXR'
            else channel
        end as brand,
        sysdate() as inserted_date_time
    from extract_channel
)

select *
from transform_brand_time
