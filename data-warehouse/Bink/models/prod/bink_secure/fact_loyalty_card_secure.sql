/*
CREATED BY:         SAM PIBWORTH
CREATED DATE:       2022-05-18
LAST MODIFIED BY:   CHRISTOPHER MITCHELL
LAST MODIFIED DATE: 2023-11-21

DESCRIPTION:
    FACT TABLE FOR LOYALTY CARD ADD & AUTH EVENTS.
	INCREMENTAL STRATEGY: LOADS ALL NEWLY INSERTED RECORDS, TRANSFORMS, THEN LOADS
	ALL LOYALTY CARD EVENTS WHICH REQUIRE UPDATING, FINALLY CALCULATING IS_MOST_RECENT
	FLAG, AND MERGING BASED ON THE EVENT ID

PARAMETERS:
    REF_OBJECT      - TRANSFORMED_HERMES_EVENTS
*/

WITH add_auth_events AS (

    SELECT *
    FROM staging.transformation.transformed_hermes_events
    WHERE (
        event_type LIKE 'lc.addandauth%'
        OR event_type LIKE 'lc.auth%'
        OR event_type LIKE 'lc.join%'
        OR event_type LIKE 'lc.register%'
        OR event_type LIKE 'lc.remove%'
        -- DON'T WANT TO PROCESS TRUSTED CHANNEL FAILURES YET
        OR event_type = 'lc.addtrusted.success'
    )
),

loyalty_plan AS (
    SELECT *
    FROM staging.staging.stg_hermes__scheme_scheme
),

add_auth_events_unpack AS (
    SELECT
        event_id,
        event_type,
        event_date_time,
        channel,
        brand,
        json:origin::VARCHAR AS origin,
        json:external_user_ref::VARCHAR AS external_user_ref,
        json:internal_user_ref::VARCHAR AS user_id,
        json:email::VARCHAR AS email,
        json:loyalty_plan::VARCHAR AS loyalty_plan,
        json:main_answer::VARCHAR AS main_answer,
        json:scheme_account_id::VARCHAR AS loyalty_card_id,
        json:consents[0]:slug::VARCHAR AS consent_slug,
        json:consents[0]:response::BOOLEAN AS consent_response
    FROM add_auth_events
),

add_auth_events_select AS (
    SELECT
        event_id,
        event_date_time,
        CASE
            WHEN event_type LIKE 'lc.addandauth%'
                THEN 'ADD AUTH'
            WHEN event_type LIKE 'lc.auth%'
                THEN 'AUTH'
            WHEN event_type LIKE 'lc.join%'
                THEN 'JOIN'
            WHEN event_type LIKE 'lc.register%'
                THEN 'REGISTER'
            WHEN event_type LIKE 'lc.remove%'
                THEN 'REMOVED'
            WHEN event_type = 'lc.addtrusted.success'
                THEN 'ADD TRUSTED'
            ELSE 'NO MATCH'
        END AS auth_type,
        CASE
            WHEN event_type LIKE '%request'
                THEN 'REQUEST'
            WHEN event_type LIKE '%success'
                THEN 'SUCCESS'
            WHEN event_type LIKE '%failed'
                THEN 'FAILED'
            WHEN event_type LIKE '%removed'
                THEN 'REMOVED'
            ELSE NULL
        END AS event_type,
        loyalty_card_id,
        loyalty_plan,
        lp.loyalty_plan_name,
        lp.loyalty_plan_company,
        NULL AS is_most_recent,
        main_answer, -- Unique identifier for schema account record,
        channel,
        brand,
        origin,
        user_id,
        external_user_ref,
        LOWER(email) AS email,
        SPLIT_PART(email, '@', 2) AS email_domain,
        consent_slug,
        consent_response,
        SYSDATE() AS inserted_date_time,
        NULL AS updated_date_time
    FROM add_auth_events_unpack e
    LEFT JOIN loyalty_plan lp ON lp.loyalty_plan_id = e.loyalty_plan
    ORDER BY event_date_time DESC
),

union_old_lc_records AS (
    SELECT *
    FROM add_auth_events_select
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
        loyalty_plan_company,
        CASE
            WHEN
                (
                    event_date_time
                    = MAX(event_date_time) OVER (PARTITION BY loyalty_card_id)
                )
                THEN TRUE
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
        consent_slug,
        consent_response,
        inserted_date_time,
        SYSDATE() AS updated_date_time
    FROM union_old_lc_records
)

SELECT *
FROM alter_is_most_recent_flag
