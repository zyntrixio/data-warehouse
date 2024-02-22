/*
Created by:         Christopher Mitchell
Created date:       2023-05-31
Last modified by:   Christopher Mitchell
Last modified date: 2023-06-06

Description:
    User table, which relates to the transform date into do date and from date for metrics layer

Parameters:
    ref_object      - src__fact_user
*/
{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID"
    )
}}

with
usr_events as (select * from {{ ref("stg_metrics__fact_user") }}

    {% if is_incremental() %}
            where user_id in 
            (
                select 
                    user_id 
                from 
                    {{ ref("stg_metrics__fact_user") }}
                where 
                    inserted_date_time >= (select max(inserted_date_time) from {{ this }})
                    )
    {% endif %}
),

usr_stage as (
    select
        event_id,
        user_id,
        coalesce(nullif(external_user_ref, ''), user_id) as user_ref,
        event_type,
        channel,
        brand,
        event_date_time,
        inserted_date_time
    from usr_events
),

to_from_date as (
    select
        event_id,
        user_id,
        user_ref,
        event_type,
        channel,
        brand,
        event_date_time as from_date,
        lead(event_date_time) over (
            partition by user_ref order by event_date_time asc
        ) as to_date,
        inserted_date_time
    from usr_stage
),

usr_final as (
    select
        event_id,
        user_ref,
        user_id,
        event_type,
        channel,
        brand,
        from_date,
        coalesce(to_date, '9999-12-31') as to_date,
        inserted_date_time,
        sysdate() as updated_date_time
    from to_from_date
)

select *
from usr_final
