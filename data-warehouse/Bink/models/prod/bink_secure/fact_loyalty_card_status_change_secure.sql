/*
Created by:         Sam Pibworth
Created date:       2022-05-19
Last modified by:
Last modified date:

Description:
    Fact table for loyalty card register request / fail / success
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all loyalty card events which require updating, finally calculating is_most_recent
	flag, and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
*/
{{ config(alias="fact_loyalty_card_status_change") }}

/* Add this back in if you want incremental models agaion - roughly 5m rows is when this becomes worth it.
{#
{{
    config(
		alias='fact_loyalty_card_status_change'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}
#}
*/
with
status_change_events as (
    select *
    from {{ ref("transformed_hermes_events") }}
    where
        event_type = 'lc.statuschange'
        {% if is_incremental() %}
            and _airbyte_emitted_at
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %}
),

account_status_lookup as (
    select * from {{ ref("stg_lookup__SCHEME_ACCOUNT_STATUS") }}
),

loyalty_plan as (select * from {{ ref("stg_hermes__SCHEME_SCHEME") }}),

status_change_events_unpack as (
    select
        event_type,
        event_date_time,
        event_id,
        channel,
        brand,
        json:origin::varchar as origin,
        json:external_user_ref::varchar as external_user_ref,
        json:internal_user_ref::varchar as user_id,
        json:email::varchar as email,
        json:scheme_account_id::varchar as loyalty_card_id,
        json:loyalty_plan::varchar as loyalty_plan_id,
        json:main_answer::varchar as main_answer,
        json:to_status::int as to_status_id
    from status_change_events
),

status_change_events_add_from_status as (
    select
        event_id,
        event_date_time,
        loyalty_card_id,
        loyalty_plan_id,
        main_answer,
        lag(to_status_id, 1) over (
            partition by loyalty_card_id order by event_date_time
        ) as from_status_id,
        to_status_id,
        channel,
        brand,
        origin,
        user_id,
        external_user_ref,
        email
    from status_change_events_unpack sce
),

status_change_events_select as (
    select
        event_id,
        event_date_time,
        loyalty_card_id,
        sce.loyalty_plan_id,
        lp.loyalty_plan_name,
        lp.loyalty_plan_company,
        from_status_id,
        asl_from.status as from_status,
        asl_from.status_type as from_status_type,
        asl_from.status_rollup as from_status_rollup,
        case
        channel when 'LLOYDS' then asl_from.api2_status else null
        end as from_external_status,
        case
        channel when 'LLOYDS' then asl_from.api2_error_slug else null
        end as from_error_slug,
        to_status_id,
        asl_to.status as to_status,
        asl_to.status_type as to_status_type,
        asl_to.status_rollup as to_status_rollup,
        case
        channel when 'LLOYDS' then asl_to.api2_status else null
        end as to_external_status,
        case
        channel when 'LLOYDS' then asl_to.api2_error_slug else null
        end as to_error_slug,
        null as is_most_recent,
        nullif(main_answer, '') as main_answer,
        origin,
        channel,
        brand,
        user_id,
        external_user_ref,
        lower(email) as email,
        split_part(email, '@', 2) as email_domain,
        sysdate() as inserted_date_time,
        null as updated_date_time
    from status_change_events_add_from_status sce
    left join loyalty_plan lp on sce.loyalty_plan_id = lp.loyalty_plan_id
    left join account_status_lookup asl_to on sce.to_status_id = asl_to.code
    left join
        account_status_lookup asl_from
        on sce.from_status_id = asl_from.code

),

union_old_lc_records as (
    select *
    from status_change_events_select
    {% if is_incremental() %}
        union
        select *
        from {{ this }}
        where
            loyalty_card_id in (
                select loyalty_card_id from status_change_events_select
            )
    {% endif %}
),

alter_is_most_recent_flag as (
    select
        event_id,
        event_date_time,
        loyalty_card_id,
        loyalty_plan_id,
        loyalty_plan_name,
        loyalty_plan_company,
        from_status_id,
        from_status,
        from_status_type,
        from_status_rollup,
        from_external_status,
        from_error_slug,
        to_status_id,
        to_status,
        to_status_type,
        to_status_rollup,
        to_external_status,
        to_error_slug,
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
        origin,
        channel,
        brand,
        user_id,
        external_user_ref,
        email,
        email_domain,
        inserted_date_time,
        sysdate() as updated_date_time
    from union_old_lc_records
)

select *
from alter_is_most_recent_flag
