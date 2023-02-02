/*
Created by:         Anand Bhakta
Created date:       2023-02-02
Last modified by:   
Last modified date: 

Description:
    Stages the freshservice table

Parameters:
    source_object      - SERVICE_DATA.FRESHSERVICE
*/


WITH
all_data as (
	SELECT
		*
	FROM
		{{ source('service_data', 'freshservice') }}
)

,all_data_select as (
	SELECT
		ID AS SERVICE_ID
		,MI
		,STATUS
		,CHANNEL
		,SERVICE
		,CREATED_AT
		,UPDATED_AT
		,SLA_BREACHED
		,_AIRBYTE_AB_ID
		,_AIRBYTE_EMITTED_AT
		,_AIRBYTE_NORMALIZED_AT
		,_AIRBYTE_FRESHSERVICE_HASHID
	FROM
		all_data
)

SELECT
	*
FROM
	all_data_select