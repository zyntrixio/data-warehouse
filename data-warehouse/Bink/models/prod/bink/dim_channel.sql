/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   Christopher Mitchell
Last modified date: 05-06-2023

Description:
    Channel table, which relates to the user_client tables in Hermes, and to channel in events

Parameters:
    ref_object      - stg_hermes__CLIENT_APPLICATION
					- stg_hermes__ORGANISATION
*/

WITH
client AS (
	SELECT *
	FROM {{ref('stg_hermes__CLIENT_APPLICATION')}}
)

,orgainsation AS (
	SELECT *
	FROM {{ref('stg_hermes__ORGANISATION')}}
)

,client_select AS (
    SELECT
		c.CHANNEL_ID
		,c.CHANNEL_NAME
		// ,c.SECRET
		,c.ORGANISATION_ID
		,o.ORGANISATION_NAME
    FROM
		client c
	LEFT JOIN
		orgainsation o
		ON c.ORGANISATION_ID = o.ORGANISATION_ID
)

,client_na_unions AS (
	SELECT
		'NOT_APPICABLE' 	AS CHANNEL_ID
		,NULL AS CHANNEL_NAME
		// ,NULL AS SECRET
		,NULL 				AS ORGANISATION_ID
		,NULL 				AS ORGANISATION_NAME
	UNION ALL
	SELECT *
	FROM client_select
)

SELECT
    *
FROM
    client_na_unions