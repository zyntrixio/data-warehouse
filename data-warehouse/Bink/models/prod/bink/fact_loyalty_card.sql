/*
Created by:         Sam Pibworth
Created date:       2022-06-15
Last modified by:   Christopher Mitchell
Last modified date: 2023-06-05

Description:
	Fact LC 

Parameters:
    ref_object      - fact_loyalty_card_secure
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
        from {{ ref("fact_loyalty_card_secure") }}
        {% if is_incremental() %}
        where updated_date_time >= (select max(updated_date_time) from {{ this }})
        {% endif %}
    ),
    lc_select as (
        select
            event_id,
            event_date_time,
            auth_type,
            event_type,
            loyalty_card_id,
            loyalty_plan,
            loyalty_plan_name,
            loyalty_plan_company,
            is_most_recent,
            case
                when main_answer is null
                then null
                when main_answer = ''
                then null
                else md5(main_answer)
            end as main_answer,
            channel,
            brand,
            origin,
            -- user_id,
            -- external_user_ref,
            email,
            email_domain,
            consent_slug,
            consent_response,
            inserted_date_time,
            updated_date_time
        from lc
    )

select *
from lc_select
