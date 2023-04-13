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
		{{ ref('stg_service_data__FRESHSERVICE') }}
)

,add_most_recent as (
	SELECT
		ID
		,TICKET_ID
		,MI
		,STATUS
		,CHANNEL
		,SERVICE
		,CREATED_AT
		,UPDATED_AT
		,SLA_BREACHED
		,CASE WHEN
			(UPDATED_AT = MAX(UPDATED_AT) OVER (PARTITION BY TICKET_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,_AIRBYTE_EMITTED_AT
		,SYSDATE() AS INSERTED_DATE_TIME
	FROM
		all_data
)


SELECT
	*
FROM
	add_most_recent
