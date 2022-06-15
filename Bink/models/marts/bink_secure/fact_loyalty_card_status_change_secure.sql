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

{{
    config(
		alias='fact_loyalty_card_status_change'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
    )
}}


WITH
status_change_events AS (
	SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	WHERE EVENT_TYPE = 'lc.statuschange'
	{% if is_incremental() %}
  	AND _AIRBYTE_NORMALIZED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)

, account_status_lookup AS (
    SELECT *
    FROM {{ref('stg_lookup__SCHEME_ACCOUNT_STATUS')}}
)

, loyalty_plan AS (
    SELECT * 
    FROM {{ref('stg_hermes__SCHEME_SCHEME')}}
)

,status_change_events_unpack AS (
	SELECT
		EVENT_TYPE
		,EVENT_DATE_TIME
		,EVENT_ID
		,JSON:origin::varchar as ORIGIN
		,JSON:channel::varchar as CHANNEL
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:email::varchar as EMAIL
		,JSON:scheme_account_id::varchar as LOYALTY_CARD_ID
		,JSON:loyalty_plan::varchar as LOYALTY_PLAN_ID
		,JSON:main_answer::varchar as MAIN_ANSWER
		,JSON:to_status::int as TO_STATUS_ID
	FROM status_change_events
)

,status_change_events_add_from_status AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN_ID
		,MAIN_ANSWER
		,LAG(TO_STATUS_ID, 1) OVER (PARTITION BY LOYALTY_CARD_ID ORDER BY EVENT_DATE_TIME) AS FROM_STATUS_ID
		,TO_STATUS_ID
		,CHANNEL
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,EMAIL
	FROM status_change_events_unpack sce
)

,status_change_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,sce.LOYALTY_PLAN_ID
		,lp.LOYALTY_PLAN_NAME
		,FROM_STATUS_ID
		,asl_from.STATUS AS FROM_STATUS
		,TO_STATUS_ID
		,asl_to.STATUS AS TO_STATUS
		,CASE WHEN MAIN_ANSWER = ''
			THEN NULL
			ELSE MAIN_ANSWER
			END AS MAIN_ANSWER
		,ORIGIN
		,CHANNEL
		,USER_ID
		,EXTERNAL_USER_REF
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS EMAIL_DOMAIN
		,SYSDATE() AS INSERTED_DATE_TIME
	FROM status_change_events_add_from_status sce
	LEFT JOIN loyalty_plan lp
		ON sce.LOYALTY_PLAN_ID = lp.LOYALTY_PLAN_ID
	LEFT JOIN account_status_lookup asl_to
		ON sce.TO_STATUS_ID = asl_to.CODE
	LEFT JOIN account_status_lookup asl_from 
		ON sce.FROM_STATUS_ID = asl_from.CODE

)


SELECT
	*
FROM
	status_change_events_select
	