/*
Created by:         Sam Pibworth
Created date:       2022-06-15
Last modified by:   
Last modified date: 

Description:
	Fact LC status change with reduced columns

Parameters:
    ref_object      - fact_loyalty_card_status_change_secure
*/
{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}

with
    lc as (
        select *
        from {{ ref("fact_loyalty_card_status_change_secure") }}
        {% if is_incremental() %}
        where updated_date_time >= (select max(updated_date_time) from {{ this }})
        {% endif %}
    ),
    lc_select as (
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
            is_most_recent,
            -- main_answer,
            origin,
            channel,
            brand,
            user_id,
            -- external_user_ref,
            -- email,
            email_domain,
            inserted_date_time,
            updated_date_time
        from lc
    )

select *
from lc_select
