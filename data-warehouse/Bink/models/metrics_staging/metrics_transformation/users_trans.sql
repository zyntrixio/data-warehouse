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
with
    usr_events as (select * from {{ ref("stg_metrics__fact_user") }}),
    usr_stage as (
        select
            user_id,
            coalesce(nullif(external_user_ref, ''), user_id) as user_ref,
            event_id,
            event_type,
            channel,
            brand,
            event_date_time
        from usr_events
    ),
    to_from_date as (
        select
            user_id,
            user_ref,
            event_id,
            event_type,
            channel,
            brand,
            event_date_time as from_date,
            lead(event_date_time) over (
                partition by user_ref order by event_date_time asc
            ) as to_date
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
            coalesce(to_date, current_timestamp) as to_date
        from to_from_date
    )

select *
from usr_final
