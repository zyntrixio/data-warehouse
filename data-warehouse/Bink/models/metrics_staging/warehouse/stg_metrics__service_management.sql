WITH
source as (
	SELECT
		*
	FROM
		{{ ref('service_management') }}
)

,renamed as (
	SELECT
		ID
		,TICKET_ID
		,MI
		,STATUS
		,CHANNEL
		,SERVICE
		,CREATED_AT AS DATE
		,UPDATED_AT
		,SLA_BREACHED
		,IS_MOST_RECENT
		//,_AIRBYTE_EMITTED_AT
		,INSERTED_DATE_TIME
	FROM
		source
)


SELECT
	*
FROM
	renamed
