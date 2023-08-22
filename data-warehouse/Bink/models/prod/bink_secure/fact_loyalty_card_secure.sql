/*
Created by:         Sam Pibworth
Created date:       2022-05-18
Last modified by:   
Last modified date: 

Description:
    Fact table for loyalty card add & auth events.
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all loyalty card events which require updating, finally calculating is_most_recent
	flag, and merging based on the event id
	
Parameters:
    ref_object      - transformed_hermes_events
*/
{{
    config(
        alias="fact_loyalty_card",
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}

with
    add_auth_events as (

        select *
        from {{ ref("transformed_hermes_events") }}
        where
            (
                event_type like 'lc.addandauth%'
                or event_type like 'lc.auth%'
                or event_type like 'lc.join%'
                or event_type like 'lc.register%'
                or event_type like 'lc.remove%'
            )

            {% if is_incremental() %}
            and _airbyte_emitted_at >= (select max(inserted_date_time) from {{ this }})
            {% endif %}
    ),
    loyalty_plan as (select * from {{ ref("stg_hermes__SCHEME_SCHEME") }}),
    add_auth_events_unpack as (
        select
            event_id,
            event_type,
            event_date_time,
            channel,
            brand,
            json:origin::varchar as origin,
            json:external_user_ref::varchar as external_user_ref,
            json:internal_user_ref::varchar as user_id,
            json:email::varchar as email,
            json:loyalty_plan::varchar as loyalty_plan,
            json:main_answer::varchar as main_answer,
            json:scheme_account_id::varchar as loyalty_card_id,
            json:consents[0]:slug::varchar as consent_slug,
            json:consents[0]:response::boolean as consent_response
        from add_auth_events
    ),
    add_auth_events_select as (
        select
            event_id,
            event_date_time,
            case
                when event_type like 'lc.addandauth%'
                then 'ADD AUTH'
                when event_type like 'lc.auth%'
                then 'AUTH'
                when event_type like 'lc.join%'
                then 'JOIN'
                when event_type like 'lc.register%'
                then 'REGISTER'
                when event_type like 'lc.remove%'
                then 'REMOVED'
                else 'NO MATCH'
            end as auth_type,
            case
                when event_type like '%request'
                then 'REQUEST'
                when event_type like '%success'
                then 'SUCCESS'
                when event_type like '%failed'
                then 'FAILED'
                when event_type like '%removed'
                then 'REMOVED'
                else null
            end as event_type,
            loyalty_card_id,
            loyalty_plan,
            lp.loyalty_plan_name,
            lp.loyalty_plan_company,
            null as is_most_recent,
            main_answer,  -- Unique identifier for schema account record,
            channel,
            brand,
            origin,
            user_id,
            external_user_ref,
            lower(email) as email,
            split_part(email, '@', 2) as email_domain,
            consent_slug,
            consent_response,
            sysdate() as inserted_date_time,
            null as updated_date_time
        from add_auth_events_unpack e
        left join loyalty_plan lp on lp.loyalty_plan_id = e.loyalty_plan
        order by event_date_time desc
    ),
    union_old_lc_records as (
        select *
        from add_auth_events_select

        {% if is_incremental() %}
        union
        select *
        from {{ this }}
        where loyalty_card_id in (select loyalty_card_id from add_auth_events_select)
        {% endif %}
    ),
    alter_is_most_recent_flag as (
        select
            event_id,
            event_date_time,
            auth_type,
            event_type,
            loyalty_card_id,
            loyalty_plan,
            loyalty_plan_name,
            loyalty_plan_company,
            case
                when
                    (
                        event_date_time
                        = max(event_date_time) over (partition by loyalty_card_id)
                    )
                then true
                else false
            end as is_most_recent,
            main_answer,
            channel,
            brand,
            origin,
            user_id,
            external_user_ref,
            email,
            email_domain,
            consent_slug,
            consent_response,
            inserted_date_time,
            sysdate() as updated_date_time
        from union_old_lc_records
    )
select *
from alter_is_most_recent_flag
