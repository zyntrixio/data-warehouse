/*
Created by:         Anand Bhakta
Created date:       2023-02-02
Last modified by:   
Last modified date: 

Description:
    Table representing all api response time data from the apistats table

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
		{{ ref('stg_service_data__APISTATS') }}
	{% if is_incremental() %}
  	WHERE _AIRBYTE_EMITTED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
	
)

,extract_channel as (
	SELECT
		API_ID
		,DATE_TIME
		,METHOD
		,PATH
		,COALESCE(
			CASE WHEN CONTAINS(CLIENT_IP, '141.92') THEN 'LBG' ELSE NULL END
			,CASE WHEN CONTAINS(CLIENT_IP, '157.83') THEN 'BARCLAYS' ELSE NULL END
					) AS CHANNEL 
		,RESPONSE_TIME
		,STATUS_CODE
		,SYSDATE() AS INSERTED_DATE_TIME
	FROM all_events
)

SELECT
	*
FROM
	extract_channel
