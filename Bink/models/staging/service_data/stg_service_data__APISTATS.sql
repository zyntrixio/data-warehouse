/*
Created by:         Anand Bhakta
Created date:       2023-02-02
Last modified by:   
Last modified date: 

Description:
    Stages the apistats table

Parameters:
    source_object      - SERVICE_DATA.APISTATS
*/

{{
    config(
        materialized='incremental'
		,unique_key='API_ID'
    )
}}

WITH
all_events as (
	SELECT
		*
	FROM
		{{ source('service_data', 'apistats') }}
	{% if is_incremental() %}
  	WHERE _AIRBYTE_EMITTED_AT >= (SELECT MAX(_AIRBYTE_EMITTED_AT) from {{ this }})
	{% endif %}
	
)

,all_events_select as (
	SELECT
		ID AS API_ID
		,PATH
		,METHOD
		//,MS_POP
		,CLIENT_IP
		,DATE_TIME
		,USER_AGENT
		,STATUS_CODE
		,RESPONSE_TIME	
		//,CLIENT_COUNTRY
		,_AIRBYTE_AB_ID
		,_AIRBYTE_EMITTED_AT
		,_AIRBYTE_NORMALIZED_AT
		,_AIRBYTE_APISTATS_HASHID
	FROM
		all_events
)

SELECT
	*
FROM
	all_events_select
