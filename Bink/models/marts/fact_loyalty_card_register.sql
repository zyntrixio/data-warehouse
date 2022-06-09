/*
Created by:         Sam Pibworth
Created date:       2022-05-19
Last modified by:   
Last modified date: 

Description:
    Fact table for loyalty card register request / fail / success

Parameters:
    ref_object      - stg_hermes__events
*/


WITH
join_events AS (
	SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	WHERE EVENT_TYPE like 'lc.register%'
	{% if is_incremental() %}
  	AND _AIRBYTE_NORMALIZED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)

,join_events_unpack AS (
	SELECT
		EVENT_ID
		,EVENT_TYPE
		,EVENT_DATE_TIME
		,JSON:origin::varchar as ORIGIN
		,JSON:channel::varchar as CHANNEL
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:email::varchar as EMAIL
		,JSON:scheme_account_id::varchar as LOYALTY_CARD_ID
		,JSON:loyalty_plan::varchar as LOYALTY_PLAN
		,JSON:main_answer::varchar as MAIN_ANSWER
		,JSON:status::int as STATUS
	FROM join_events
)

,join_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,CASE WHEN EVENT_TYPE = 'lc.register.request'
			THEN 'REQUEST'
			WHEN EVENT_TYPE = 'lc.register.success'
			THEN 'SUCCESS'
			WHEN EVENT_TYPE = 'lc.register.failed'
			THEN 'FAILED'
			ELSE NULL
			END AS EVENT_TYPE
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY LOYALTY_CARD_ID)) // Need to think about simeultaneous events - rank by business logic
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,CASE WHEN MAIN_ANSWER = '' // Unique identifier for schema account record - this is empty???
			THEN NULL
			ELSE MAIN_ANSWER
			END AS MAIN_ANSWER
		,STATUS
		,CHANNEL
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS EMAIL_DOMAIN
		,SYSDATE() AS INSERTED_DATE_TIME
	FROM join_events_unpack
	ORDER BY EVENT_DATE_TIME DESC
)


SELECT
	*
FROM
	join_events_select
	