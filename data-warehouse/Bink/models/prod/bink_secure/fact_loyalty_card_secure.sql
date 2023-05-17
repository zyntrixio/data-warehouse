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
{{ config(
    alias = 'fact_loyalty_card',
    materialized = 'incremental',
    unique_key = 'EVENT_ID',
    merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
) }}

WITH add_auth_events AS (

    SELECT
        *
    FROM
        {{ ref('transformed_hermes_events') }}
    WHERE
        (
            event_type LIKE 'lc.addandauth%'
            OR event_type LIKE 'lc.auth%'
            OR event_type LIKE 'lc.join%'
            OR event_type LIKE 'lc.register%'
            OR event_type LIKE 'lc.remove%'
        )

{% if is_incremental() %}
AND _AIRBYTE_EMITTED_AT >= (
    SELECT
        MAX(inserted_date_time)
    FROM
        {{ this }}
)
{% endif %}
),
loyalty_plan AS (
    SELECT
        *
    FROM
        {{ ref('stg_hermes__SCHEME_SCHEME') }}
),
add_auth_events_unpack AS (
    SELECT
        event_id,
        event_type,
        event_date_time,
        channel,
        brand,
        json :origin :: VARCHAR AS origin,
        json :external_user_ref :: VARCHAR AS external_user_ref,
        json :internal_user_ref :: VARCHAR AS user_id,
        json :email :: VARCHAR AS email,
        json :loyalty_plan :: VARCHAR AS loyalty_plan,
        json :main_answer :: VARCHAR AS main_answer,
        json :scheme_account_id :: VARCHAR AS loyalty_card_id
    FROM
        add_auth_events
),
add_auth_events_select AS (
    SELECT
        event_id,
        event_date_time,CASE
            WHEN event_type LIKE 'lc.addandauth%' THEN 'ADD AUTH'
            WHEN event_type LIKE 'lc.auth%' THEN 'AUTH'
            WHEN event_type LIKE 'lc.join%' THEN 'JOIN'
            WHEN event_type LIKE 'lc.register%' THEN 'REGISTER'
            WHEN event_type LIKE 'lc.remove%' THEN 'REMOVED'
            ELSE 'NO MATCH'
        END AS auth_type,CASE
            WHEN event_type LIKE '%request' THEN 'REQUEST'
            WHEN event_type LIKE '%success' THEN 'SUCCESS'
            WHEN event_type LIKE '%failed' THEN 'FAILED'
            WHEN event_type LIKE '%removed' THEN 'REMOVED'
            ELSE NULL
        END AS event_type,
        loyalty_card_id,
        loyalty_plan,
        lp.loyalty_plan_name,
        lp.LOYALTY_PLAN_COMPANY,
        NULL AS is_most_recent,
        main_answer, -- Unique identifier for schema account record,
        channel,
        brand,
        origin,
        user_id,
        external_user_ref,
        LOWER(email) AS email,
        SPLIT_PART(
            email,
            '@',
            2
        ) AS email_domain,
        SYSDATE() AS inserted_date_time,
        NULL AS updated_date_time
    FROM
        add_auth_events_unpack e
        LEFT JOIN loyalty_plan lp
        ON lp.loyalty_plan_id = e.loyalty_plan
    ORDER BY
        event_date_time DESC
),
union_old_lc_records AS (
    SELECT
        *
    FROM
        add_auth_events_select

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    {{ this }}
WHERE
    loyalty_card_id IN (
        SELECT
            loyalty_card_id
        FROM
            add_auth_events_select
    )
{% endif %}
),
alter_is_most_recent_flag AS (
    SELECT
        event_id,
        event_date_time,
        auth_type,
        event_type,
        loyalty_card_id,
        loyalty_plan,
        loyalty_plan_name,
        LOYALTY_PLAN_COMPANY,
        CASE
            WHEN (event_date_time = MAX(event_date_time) over (PARTITION BY loyalty_card_id)) THEN TRUE
            ELSE FALSE
        END AS is_most_recent,
        main_answer,
        channel,
        brand,
        origin,
        user_id,
        external_user_ref,
        email,
        email_domain,
        inserted_date_time,
        SYSDATE() AS updated_date_time
    FROM
        union_old_lc_records
)
SELECT
    *
FROM
    alter_is_most_recent_flag
