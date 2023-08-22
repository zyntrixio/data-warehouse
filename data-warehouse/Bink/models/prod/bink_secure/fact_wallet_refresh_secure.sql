/*
Created by:         Anand Bhakta
Created date:       2023-05-17
Last modified by:
Last modified date:

Description:
    Loads user wallet refresh events from event table
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all user events which require updating, finally calculating is_most_recent flag,
	and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
*/
{{
    config(
        alias="fact_wallet_refresh",
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}

with
user_events as (
    select *
    from {{ ref("transformed_hermes_events") }}
    where
        event_type = 'user.session.start'
        {% if is_incremental() %}
            and _airbyte_emitted_at
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %}
),

user_events_unpack as (
    select
        event_id,
        event_type,
        event_date_time,
        channel,
        brand,
        json:internal_user_ref::varchar as user_id,
        json:origin::varchar as origin,
        json:external_user_ref::varchar as external_user_ref,
        json:email::varchar as email
    from user_events
),

user_events_select as (
    select
        event_id,
        event_date_time,
        user_id,
        case
            when event_type = 'user.session.start' then 'REFRESH' else null
        end as event_type,
        null as is_most_recent,
        origin,
        channel,
        brand,
        nullif(external_user_ref, '') as external_user_ref,
        lower(email) as email,
        split_part(email, '@', 2) as domain,
        sysdate() as inserted_date_time,
        null as updated_date_time
    from user_events_unpack
),

union_old_user_records as (
    select *
    from user_events_select
    {% if is_incremental() %}
        union
        select *
        from {{ this }}
        where user_id in (select user_id from user_events_select)
    {% endif %}
),

alter_is_most_recent_flag as (
    select
        event_id,
        event_date_time,
        user_id,
        event_type,
        null as is_most_recent,
        origin,
        channel,
        brand,
        external_user_ref,
        email,
        domain,
        inserted_date_time,
        sysdate() as updated_date_time
    from union_old_user_records
)

select *
from alter_is_most_recent_flag
