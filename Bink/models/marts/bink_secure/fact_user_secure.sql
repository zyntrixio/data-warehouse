/*
Created by:         Sam Pibworth
Created date:       2022-05-04
Last modified by:   
Last modified date: 

Description:
    extracts user created and user deleted from the events table and 

Parameters:
    ref_object      - stg_hermes__events
*/

{{
    config(
		alias='fact_user'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
user_events AS (
	SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	WHERE EVENT_TYPE LIKE 'user%'
	{% if is_incremental() %}
  	AND _AIRBYTE_NORMALIZED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}	
)

,user_events_unpack AS (
	SELECT
		EVENT_ID
		,EVENT_TYPE
		,EVENT_DATE_TIME
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:origin::varchar as ORIGIN
		,JSON:channel::varchar as CHANNEL
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:email::varchar as EMAIL		
	FROM user_events
)

,user_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,USER_ID
		,CASE WHEN EVENT_TYPE = 'user.created'
			THEN 'CREATED'
			WHEN EVENT_TYPE = 'user.deleted'
			THEN 'DELETED'
			ELSE NULL
			END AS EVENT_TYPE
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY USER_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,ORIGIN
		,CHANNEL
		,NULLIF(EXTERNAL_USER_REF,'') AS EXTERNAL_USER_REF
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS DOMAIN
		,SYSDATE() AS INSERTED_DATE_TIME
	FROM user_events_unpack
)

,union_old_user_records AS (
	SELECT *
	FROM user_events_select
	{% if is_incremental() %}
	UNION
	SELECT *
	FROM {{ this }}
	WHERE USER_ID IN (
		SELECT USER_ID
		FROM user_events_select
	)
	{% endif %}
)

,alter_is_most_recent_flag AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,USER_ID
		,EVENT_TYPE
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY USER_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,ORIGIN
		,CHANNEL
		,EXTERNAL_USER_REF
		,EMAIL
		,DOMAIN
		,INSERTED_DATE_TIME
		,SYSDATE() AS UPDATED_DATE_TIME
	FROM
		union_old_user_records
)

SELECT
	*
FROM
	alter_is_most_recent_flag
	