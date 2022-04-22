/*
Created by:         Sam Pibworth
Created date:       2022-04-22
Last modified by:   
Last modified date: 

Description:
    Stages the client_application table

Parameters:
    source_object      - HERMES.USER_CLIENTAPPLICATION
*/

WITH client_application AS (
	SELECT
		CLIENT_ID :: VARCHAR AS CHANNEL_ID
		,NAME AS CHANNEL_NAME
		,SECRET
		,ORGANISATION_ID

	FROM
		{{ source('Hermes', 'CLIENT_APPLICATION') }}
)

SELECT
	*
FROM
	client_application