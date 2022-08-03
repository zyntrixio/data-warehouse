/*
Created by:         Sam Pibworth
Created date:       2022-05-18
Last modified by:   
Last modified date: 

Description:
    Fact table for loyalty card add & auth events

Parameters:
    ref_object      - stg_hermes__events
*/

{{
    config(
		alias='fact_loyalty_card_add_auth'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT']
    )
}}

WITH
add_auth_events AS (
	SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	WHERE EVENT_TYPE like 'lc.addandauth%'
	{% if is_incremental() %}
  	AND _AIRBYTE_NORMALIZED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)

,add_auth_events_unpack AS (
	SELECT
		EVENT_ID
		,EVENT_TYPE
		,EVENT_DATE_TIME
		,JSON:origin::varchar as ORIGIN
		,JSON:channel::varchar as CHANNEL
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:email::varchar as EMAIL
		,JSON:loyalty_plan::varchar as LOYALTY_PLAN
		,JSON:main_answer::varchar as MAIN_ANSWER
		,JSON:scheme_account_id::varchar as LOYALTY_CARD_ID
	FROM add_auth_events
)

,add_auth_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,CASE WHEN EVENT_TYPE = 'lc.addandauth.request'
			THEN 'REQUEST'
			WHEN EVENT_TYPE = 'lc.addandauth.success'
			THEN 'SUCCESS'
			WHEN EVENT_TYPE = 'lc.addandauth.failed'
			THEN 'FAILED'
			ELSE NULL
			END AS EVENT_TYPE
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,FALSE AS IS_MOST_RECENT
		,MAIN_ANSWER // Unique identifier for schema account record
		,CHANNEL
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS EMAIL_DOMAIN
		,SYSDATE() AS INSERTED_DATE_TIME
	FROM add_auth_events_unpack
	ORDER BY EVENT_DATE_TIME DESC
)

,union_old_lc_records AS (
	SELECT *
	FROM add_auth_events_select
	{% if is_incremental() %}
	UNION
	SELECT *
	FROM {{ this }}
	WHERE LOYALTY_CARD_ID IN (
		SELECT LOYALTY_CARD_ID
		FROM add_auth_events_select
	)
	{% endif %}
)

,alter_is_most_recent_flag AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,EVENT_TYPE
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY LOYALTY_CARD_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,MAIN_ANSWER
		,CHANNEL
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,EMAIL
		,EMAIL_DOMAIN
		,INSERTED_DATE_TIME
	FROM
		union_old_lc_records
)

SELECT
	*
FROM
	alter_is_most_recent_flag