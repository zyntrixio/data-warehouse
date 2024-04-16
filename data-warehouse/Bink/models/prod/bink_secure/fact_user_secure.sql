/*
Created by:         Sam Pibworth
Created date:       2022-05-04
Last modified by:   Anand Bhakta
Last modified date: 2023-11-20

Description:
    Loads user created and user deleted from the events table
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all user events which require updating, finally calculating is_most_recent flag,
	and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
*/
{{
    config(
        alias="fact_user",
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
        event_type like 'user%' and event_type not in ('user.session.start', 'user.wallet_view')
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
        md5(json:external_user_ref::varchar) as external_user_ref,
        json:email::varchar as email
    from user_events
),

user_events_select as (
    select
        event_id,
        event_date_time,
        user_id,
        case
            when event_type = 'user.created'
                then 'CREATED'
            when event_type = 'user.deleted'
                then 'DELETED'
            when event_type = 'user.RTBF'
                then 'RTBF'
            else null
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
