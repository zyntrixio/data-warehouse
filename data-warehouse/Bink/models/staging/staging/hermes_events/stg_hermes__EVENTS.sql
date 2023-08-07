/*
Created by:         Sam Pibworth
Created date:       2022-05-04
Last modified by:   
Last modified date: 

Description:
    Stages the events table, which is an aggregation of all hermes events including user, payment, and loyalty card information

Parameters:
    source_object      - HERMES_EVENTS.EVENTS
*/

{{
    config(
        materialized='incremental'
		,unique_key='EVENT_ID'
    )
}}

WITH
all_events as (
	SELECT
		*
	FROM
		{{ source('snowstorm', 'events') }} --this is pointing to service_data schema
	{% if is_incremental() %}
  	WHERE _AIRBYTE_EMITTED_AT >= (SELECT MAX(_AIRBYTE_EMITTED_AT) from {{ this }})
	{% endif %}
	
)

,all_events_select as (
	SELECT
		PARSE_JSON(JSON) AS JSON
		,EVENT_TYPE
		,EVENT_DATE_TIME::TIMESTAMP AS EVENT_DATE_TIME
		,_AIRBYTE_AB_ID
		,_AIRBYTE_EMITTED_AT
		,_AIRBYTE_NORMALIZED_AT
		,_AIRBYTE_EVENTS_HASHID
		-- ,_AIRBYTE_UNIQUE_KEY
		,ID AS EVENT_ID
	FROM
		all_events
)

SELECT
	*
FROM
	all_events_select
