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
        materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
user_events AS (
	SELECT *
	FROM {{ ref('fact_user_secure')}}
	{% if is_incremental() %}
  	WHERE UPDATED_DATE_TIME>= (SELECT MAX(UPDATED_DATE_TIME) from {{ this }})
	{% endif %}
)

,user_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,USER_ID
		,EVENT_TYPE
		,IS_MOST_RECENT
		,ORIGIN
		,CHANNEL
		// ,EXTERNAL_USER_REF
		// ,LOWER(EMAIL) AS EMAIL
		,INSERTED_DATE_TIME
		,UPDATED_DATE_TIME
	FROM user_events
)

SELECT
	*
FROM
	user_events_select
	