/*
Created by:         Anand Bhakta
Created date:       2023-05-17
Last modified by:   
Last modified date: 

Description:
    Loads user wallet refresh events from event table
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all user events which require updating, finally calculating is_most_recent flag,
	and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
*/

{{
    config(
		alias='fact_wallet_refresh'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
user_events AS (
	SELECT *
	FROM {{ ref('transformed_hermes_events')}}
	WHERE EVENT_TYPE = 'user.session.start'
	{% if is_incremental() %}
  	AND _AIRBYTE_EMITTED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}	
)

,user_events_unpack AS (
	SELECT
		EVENT_ID
		,EVENT_TYPE
		,EVENT_DATE_TIME
	    ,CHANNEL
        ,BRAND
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:origin::varchar as ORIGIN
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:email::varchar as EMAIL		
	FROM user_events
)

,user_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,USER_ID
		,CASE WHEN EVENT_TYPE = 'user.session.start'
			THEN 'REFRESH'
			ELSE NULL
			END AS EVENT_TYPE
		,NULL AS IS_MOST_RECENT
		,ORIGIN
		,CHANNEL
        ,BRAND
		,NULLIF(EXTERNAL_USER_REF,'') AS EXTERNAL_USER_REF
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS DOMAIN
		,SYSDATE() AS INSERTED_DATE_TIME
		,NULL AS UPDATED_DATE_TIME
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
		,NULL AS IS_MOST_RECENT
		,ORIGIN
		,CHANNEL
        ,BRAND
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
	