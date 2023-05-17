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
		{{ dbt_utils.surrogate_key(
      ['ID',
      'UPDATED_AT']
  			) }} as ID
		,ID AS TICKET_ID
		,MI
		,STATUS
		,CHANNEL
		,SERVICE
		,CREATED_AT
		,UPDATED_AT
		,SLA_BREACHED
		,_AIRBYTE_EMITTED_AT
	FROM
		all_data
)

SELECT
	*
FROM
	all_data_select
