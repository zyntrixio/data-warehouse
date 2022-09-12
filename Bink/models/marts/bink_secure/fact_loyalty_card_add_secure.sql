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
    ref_object      - stg_hermes__events
*/

{{
    config(
		alias='fact_loyalty_card_add'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
add_auth_events AS (
	SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	WHERE ( EVENT_TYPE like 'lc.addandauth%' or EVENT_TYPE like 'lc.auth%'  or EVENT_TYPE like 'lc.join%' or EVENT_TYPE like 'lc.register%')
	{% if is_incremental() %}
  	AND _AIRBYTE_EMITTED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
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
        ,case when EVENT_TYPE like 'lc.addandauth%' then 'ADD AUTH'
            when EVENT_TYPE like 'lc.auth%' then 'AUTH'
            when EVENT_TYPE like 'lc.join%'  then 'JOIN'
            when EVENT_TYPE like 'lc.register%' then 'REGISTER'
            else 'NO MATCH'
        end as auth_type
		,CASE WHEN EVENT_TYPE like '%request'
			THEN 'REQUEST'
			WHEN EVENT_TYPE like '%success'
			THEN 'SUCCESS'
			WHEN EVENT_TYPE like '%failed'
			THEN 'FAILED'
			ELSE NULL
			END AS EVENT_TYPE
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,NULL AS IS_MOST_RECENT
		,MAIN_ANSWER -- Unique identifier for schema account record
		,CHANNEL
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS EMAIL_DOMAIN
		,SYSDATE() AS INSERTED_DATE_TIME
		,NULL AS UPDATED_DATE_TIME
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
		,auth_type
		,EVENT_TYPE
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
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
		,SYSDATE() AS UPDATED_DATE_TIME
	FROM
		union_old_lc_records
)

SELECT
	*
FROM
	alter_is_most_recent_flag

-- union all

-- select 000000001 as event_id,
-- to_timestamp('2020-12-11 19:08:25.283') as EVENT_DATE_TIME,
-- 'AUTH' as auth_type,
-- 'SUCCESS' as EVENT_TYPE,
-- '349429' as LOYALTY_CARD_ID,
-- null as LOYALTY_PLAN,
-- 'FALSE'  as is_most_recent
-- ,null as MAIN_ANSWER
-- ,'com.barclays.bmb' as CHANNEL
-- ,null as ORIGIN
-- ,'0000001' as USER_ID
-- , null as EXTERNAL_USER_REF
-- ,null  as EMAIL
-- ,null as EMAIL_DOMAIN
-- ,SYSDATE() as INSERTED_DATE_TIME
-- ,SYSDATE() AS UPDATED_DATE_TIME


-- union all 



-- select 000000002 as event_id,
-- to_timestamp('2020-12-13 19:08:25.283') as EVENT_DATE_TIME,
-- 'AUTH' as auth_type,
-- 'SUCCESS' as EVENT_TYPE,
-- '349429' as LOYALTY_CARD_ID,
-- null as LOYALTY_PLAN,
-- 'FALSE'  as is_most_recent
-- ,null as MAIN_ANSWER
-- ,'barclays' as CHANNEL
-- ,null as ORIGIN
-- ,'0000001' as USER_ID
-- , null as EXTERNAL_USER_REF
-- ,null  as EMAIL
-- ,null as EMAIL_DOMAIN
-- ,SYSDATE() as INSERTED_DATE_TIME
-- ,SYSDATE() AS UPDATED_DATE_TIME



