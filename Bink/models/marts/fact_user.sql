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


WITH

user_events AS (
	SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	WHERE EVENT_TYPE IN ('user.created', 'user.deleted')
)

,user_events_select AS (
	SELECT
		JSON:internal_user_ref::varchar as USER_ID
		,CASE WHEN EVENT_TYPE = 'user.created'
			THEN 'CREATED'
			WHEN EVENT_TYPE = 'user.deleted'
			THEN 'DELETED'
			ELSE NULL
			END AS EVENT_TYPE
		,EVENT_DATE_TIME
		,JSON:origin::varchar as ORIGIN
		,JSON:channel::varchar as CHANNEL
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:email::varchar as EMAIL		
	FROM user_events
)


SELECT
	*
FROM
	user_events_select
	