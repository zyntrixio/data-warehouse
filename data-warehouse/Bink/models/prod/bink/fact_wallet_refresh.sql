/*
Created by:         Anand Bhakta
Created date:       2023-05-17
Last modified by:   
Last modified date: 

Description:
    extracts user wallet refresh events table and 

Parameters:
    ref_object      - fact_wallet_refresh_secure
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
	FROM {{ ref('fact_wallet_refresh_secure')}}
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
		,BRAND
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
	