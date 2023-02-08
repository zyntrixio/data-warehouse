/*
Created by:         Sam Pibworth
Created date:       2022-05-19
Last modified by:   
Last modified date: 

Description:
    Fact table for loyalty card register request / fail / success
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all loyalty card events which require updating, finally calculating is_most_recent
	flag, and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
*/

{{
    config(
		alias='fact_loyalty_card_register'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
register_events AS (
	SELECT *
	FROM {{ ref('transformed_hermes_events')}}
	WHERE EVENT_TYPE like 'lc.register%'
	{% if is_incremental() %}
  	AND _AIRBYTE_EMITTED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)

,loyalty_plan AS (
	SELECT *
	FROM {{ ref('stg_hermes__SCHEME_SCHEME')}}
)

,register_events_unpack AS (
	SELECT
		EVENT_ID
		,EVENT_TYPE
		,EVENT_DATE_TIME
		,CHANNEL
        ,BRAND
        ,JSON:origin::varchar as ORIGIN
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:email::varchar as EMAIL
		,JSON:scheme_account_id::varchar as LOYALTY_CARD_ID
		,JSON:loyalty_plan::varchar as LOYALTY_PLAN
		,JSON:main_answer::varchar as MAIN_ANSWER
		,JSON:status::int as STATUS
	FROM register_events
)

,register_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,lp.LOYALTY_PLAN_NAME
		,CASE WHEN EVENT_TYPE = 'lc.register.request'
			THEN 'REQUEST'
			WHEN EVENT_TYPE = 'lc.register.success'
			THEN 'SUCCESS'
			WHEN EVENT_TYPE = 'lc.register.failed'
			THEN 'FAILED'
			ELSE NULL
			END AS EVENT_TYPE
		,NULL AS IS_MOST_RECENT
		,CASE WHEN MAIN_ANSWER = '' // Unique identifier for schema account record - this is empty???
			THEN NULL
			ELSE MAIN_ANSWER
			END AS MAIN_ANSWER
		,STATUS
		,CHANNEL
        ,BRAND
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS EMAIL_DOMAIN
		,SYSDATE() AS INSERTED_DATE_TIME
		,NULL AS UPDATED_DATE_TIME
	FROM 
		register_events_unpack e
	LEFT JOIN
		loyalty_plan lp ON lp.LOYALTY_PLAN_ID = e.LOYALTY_PLAN
	ORDER BY EVENT_DATE_TIME DESC
)

,union_old_lc_records AS (
	SELECT *
	FROM register_events_select
	{% if is_incremental() %}
	UNION
	SELECT *
	FROM {{ this }}
	WHERE LOYALTY_CARD_ID IN (
		SELECT LOYALTY_CARD_ID
		FROM register_events_select
	)
	{% endif %}
)

,alter_is_most_recent_flag AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,LOYALTY_PLAN_NAME
		,EVENT_TYPE
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY LOYALTY_CARD_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,MAIN_ANSWER
		,STATUS
		,CHANNEL
        ,BRAND
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,EMAIL
		,EMAIL_DOMAIN
		,INSERTED_DATE_TIME
		,SYSDATE() AS UPDATED_DATE_TIME
	FROM
		union_old_lc_records
)

SELECT
	*
FROM
	alter_is_most_recent_flag
	