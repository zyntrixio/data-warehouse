/*
Created by:         Sam Pibworth
Created date:       2022-05-05
Last modified by:
Last modified date:

Description:
    Extracts payment_account_status_change from the events table
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all payment account events which require updating, finally calculating is_most_recent
	flag, and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
*/
{{
    config(
        alias="fact_payment_account_status_change",
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}

with
payment_events as (
    select *
    from {{ ref("transformed_hermes_events") }}
    where
        event_type = 'payment.account.status.change'
        {% if is_incremental() %}
            and _airbyte_emitted_at
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %}
),

payment_account_status_lookup as (
    select * from {{ ref("stg_lookup__PAYMENT_ACCOUNT_STATUS") }}
),

payment_events_unpack as (
    select
        event_id,
        event_type,
        event_date_time,
        channel,
        brand,
        json:origin::varchar as origin,
        md5(json:external_user_ref::varchar) as external_user_ref,
        json:internal_user_ref::varchar as user_id,
        json:email::varchar as email,
        json:payment_account_id::varchar as payment_account_id,
        json:expiry_date::varchar as expiry_date,
        json:token::varchar as token,
        json:from_status::integer as from_status_id,
        json:to_status::integer as to_status_id
    from payment_events
),

payment_events_join_status as (
    select
        event_id,
        event_date_time,
        payment_account_id,
        origin,
        channel,
        brand,
        user_id,
        external_user_ref,
        expiry_date,
        token,
        from_status_id,
        s_from.status as from_status,
        to_status_id,
        s_to.status as to_status,
        email
    from payment_events_unpack
    left join
        payment_account_status_lookup s_from
        on from_status_id = s_from.payment_status_id
    left join
        payment_account_status_lookup s_to
        on to_status_id = s_to.payment_status_id
),

payment_events_select as (
    select
        event_id,
        event_date_time,
        payment_account_id,
        null as is_most_recent,
        from_status_id,
        from_status,
        to_status_id,
        to_status,
        origin,
        channel,
        brand,
        user_id,
        external_user_ref,
        split_part(expiry_date, '/', 1)::integer as expiry_month,
        case
            when split_part(expiry_date, '/', 2)::integer >= 2000
                then split_part(expiry_date, '/', 2)::integer
            else split_part(expiry_date, '/', 2)::integer + 2000
        end as expiry_year,
        concat(expiry_year, '-', expiry_month) as expiry_year_month,
        token,
        lower(email) as email,
        split_part(email, '@', 2) as email_domain,
        sysdate() as inserted_date_time,
        null as updated_date_time
    from payment_events_join_status
),

union_old_pa_records as (
    select *
    from payment_events_select
    {% if is_incremental() %}
        union
        select *
        from {{ this }}
        where
            payment_account_id in (
                select payment_account_id from payment_events_select
            )
    {% endif %}
),

alter_is_most_recent_flag as (
    select
        event_id,
        event_date_time,
        payment_account_id,
        case
            when
                (
                    event_date_time
                    = max(event_date_time)
                        over (partition by payment_account_id)
                )
                then true
            else false
        end as is_most_recent,
        from_status_id,
        from_status,
        to_status_id,
        to_status,
        origin,
        channel,
        brand,
        user_id,
        external_user_ref,
        expiry_month,
        expiry_year,
        expiry_year_month,
        token,
        email,
        email_domain,
        inserted_date_time,
        sysdate() as updated_date_time
    from union_old_pa_records
)

select *
from alter_is_most_recent_flag
