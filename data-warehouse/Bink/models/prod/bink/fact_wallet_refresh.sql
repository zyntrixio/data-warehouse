/*
Created by:         Anand Bhakta
Created date:       2023-05-17
Last modified by:   
Last modified date: 

Description:
    extracts user wallet refresh events table and 

Parameters:
    ref_object      - fact_wallet_refresh_secure
*/
{{
    config(
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}

with
    user_events as (
        select *
        from {{ ref("fact_wallet_refresh_secure") }}
        {% if is_incremental() %}
        where updated_date_time >= (select max(updated_date_time) from {{ this }})
        {% endif %}
    ),
    user_events_select as (
        select
            event_id,
            event_date_time,
            user_id,
            event_type,
            is_most_recent,
            origin,
            channel,
            brand,
            -- external_user_ref,
            -- lower(email) as email,
            inserted_date_time,
            updated_date_time
        from user_events
    )

select *
from user_events_select
