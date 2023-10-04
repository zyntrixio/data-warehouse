/*
Created by:         Anand Bhakta
Created date:       2023-07-04
Last modified by:
Last modified date:

Description:
    Fact table for loyalty card and payment card pll link status
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all events which require updating, finally calculating is_most_recent
	flag, and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
					- stg_hermes__CLIENT_APPLICATION
					- stg_hermes__SCHEME_SCHEMEACCOUNT
					- stg_hermes__SCHEME_SCHEME

*/
{{
    config(
        alias="fact_pll_link_status_change",
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}

with
status_change_events as (
    select *
    from {{ ref("transformed_hermes_events") }}
    where
        event_type = 'pll_link.statuschange'
        {% if is_incremental() %}
            and _airbyte_emitted_at
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %}
),

dim_channel as (select * from {{ ref("stg_hermes__CLIENT_APPLICATION") }}),

dim_loyalty as (select * from {{ ref("stg_hermes__SCHEME_SCHEMEACCOUNT") }}),

dim_loyalty_plan as (select * from {{ ref("stg_hermes__SCHEME_SCHEME") }}),

status_change_events_unpack as (
    select
        event_type,
        event_date_time,
        event_id,
        c.channel_name,
        json:origin::varchar as origin,
        json:external_user_ref::varchar as external_user_ref,
        json:internal_user_ref::varchar as user_id,
        json:scheme_account_id::varchar as loyalty_card_id,
        json:payment_account_id::varchar as payment_account_id,
        json:from_state::int as from_status_id,
        json:to_state::int as to_status_id
    from status_change_events s
    left join dim_channel c on c.channel_id = s.channel
),

status_change_events_case as (
    select
        event_id,
        event_date_time,
        sce.loyalty_card_id,
        p.loyalty_plan_id,
        p.loyalty_plan_company,
        p.loyalty_plan_name,
        payment_account_id,
        from_status_id,
        case
        from_status_id
            when 0
                then 'PENDING'
            when 1
                then 'ACTIVE'
            when 2
                then 'INACTIVE'
            else 'INACTIVE'
        end as from_status,
        to_status_id,
        case
        to_status_id
            when 0
                then 'PENDING'
            when 1
                then 'ACTIVE'
            when 2
                then 'INACTIVE'
            else 'INACTIVE'
        end as to_status,
        case
            when channel_name in ('Bank of Scotland', 'Lloyds', 'Halifax')
                then 'LLOYDS'
            when channel_name = 'Barclays Mobile Banking'
                then 'BARCLAYS'
            else upper(channel_name)
        end as channel,
        case
        channel_name
            when 'Bank of Scotland'
                then 'BOS'
            when 'Barclays Mobile Banking'
                then 'BARCLAYS'
            else upper(channel_name)
        end as brand,
        origin,
        user_id,
        external_user_ref,
        null as is_most_recent,
        sysdate() as inserted_date_time,
        null as updated_date_time
    from status_change_events_unpack sce
    left join dim_loyalty l on l.loyalty_card_id = sce.loyalty_card_id
    left join dim_loyalty_plan p on p.loyalty_plan_id = l.loyalty_plan_id
    qualify
        not (
            equal_null(
                from_status_id,
                lag(from_status_id, 1) over (
                    partition by sce.loyalty_card_id, payment_account_id
                    order by event_date_time asc
                )
            )
            and equal_null(
                to_status_id,
                lag(to_status_id, 1) over (
                    partition by sce.loyalty_card_id, payment_account_id
                    order by event_date_time asc
                )
            )
        )  -- REMOVING DUPLICATES
),

union_old_lc_records as (
    select *
    from status_change_events_case
    {% if is_incremental() %}
        union
        select *
        from {{ this }}
        where
            (loyalty_card_id, payment_account_id) in (
                select
                    loyalty_card_id,
                    payment_account_id
                from status_change_events_case
            )
    {% endif %}
),

alter_is_most_recent_flag as (
    select
        event_id,
        event_date_time,
        loyalty_card_id,
        loyalty_plan_id,
        loyalty_plan_company,
        loyalty_plan_name,
        payment_account_id,
        from_status_id,
        from_status,
        to_status_id,
        to_status,
        channel,
        brand,
        origin,
        user_id,
        external_user_ref,
        case
            when
                (
                    event_date_time = max(event_date_time) over (
                        partition by loyalty_card_id, payment_account_id
                    )
                )
                then true
            else false
        end as is_most_recent,
        inserted_date_time,
        sysdate() as updated_date_time
    from union_old_lc_records
)

select *
from alter_is_most_recent_flag
