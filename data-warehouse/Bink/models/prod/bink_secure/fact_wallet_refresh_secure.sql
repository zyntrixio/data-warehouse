/*
CREATED BY:         ANAND BHAKTA
CREATED DATE:       2023-05-17
LAST MODIFIED BY:   CHRISTOPHER MITCHELL
LAST MODIFIED DATE: 2023-11-21

DESCRIPTION:
    LOADS USER WALLET REFRESH EVENTS FROM EVENT TABLE
	INCREMENTAL STRATEGY: LOADS ALL NEWLY INSERTED RECORDS, TRANSFORMS, THEN LOADS
	ALL USER EVENTS WHICH REQUIRE UPDATING, FINALLY CALCULATING IS_MOST_RECENT FLAG,
	AND MERGING BASED ON THE EVENT ID

PARAMETERS:
    REF_OBJECT      - TRANSFORMED_HERMES_EVENTS
*/
{{
    config(
        alias="fact_wallet_refresh",
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}
WITH user_events AS (
    SELECT *
    FROM {{ ref('transformed_hermes_events') }}
    WHERE event_type IN ('user.wallet_view', 'user.session.start')
    {% if is_incremental() %}
            and _airbyte_emitted_at
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %}
),

user_events_unpack AS (
    SELECT
        event_id,
        event_type,
        event_date_time,
        channel,
        brand,
        json:internal_user_ref::VARCHAR AS user_id,
        json:origin::VARCHAR AS origin,
        md5(json:external_user_ref::varchar) as external_user_ref,
        json:email::VARCHAR AS email
    FROM user_events
),

user_events_select AS (
    SELECT
        event_id,
        event_date_time,
        user_id,
        CASE
            WHEN event_type = 'user.session.start' THEN 'WALLET_REFRESH'
            WHEN event_type = 'user.wallet_view' THEN 'WALLET_VIEW'
            ELSE NULL
        END AS event_type,
        NULL AS is_most_recent,
        origin,
        channel,
        brand,
        NULLIF(external_user_ref, '') AS external_user_ref,
        LOWER(email) AS email,
        SPLIT_PART(email, '@', 2) AS domain,
        SYSDATE() AS inserted_date_time,
        NULL AS updated_date_time
    FROM user_events_unpack
),

union_old_user_records AS (
    SELECT *
    FROM user_events_select
    {% if is_incremental() %}
        union
        select *
        from {{ this }}
        where user_id in (select user_id from user_events_select)
    {% endif %}
),

alter_is_most_recent_flag AS (
    SELECT
        event_id,
        event_date_time,
        user_id,
        event_type,
        NULL AS is_most_recent,
        origin,
        channel,
        brand,
        external_user_ref,
        email,
        domain,
        inserted_date_time,
        SYSDATE() AS updated_date_time
    FROM union_old_user_records
)

SELECT *
FROM alter_is_most_recent_flag
