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

SELECT
	*
FROM
	all_data