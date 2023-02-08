/*
Created by:         Sam Pibworth
Created date:       2022-04-22
Last modified by:   
Last modified date: 

Description:
    Stages the organisation table

Parameters:
    source_object      - HERMES.USER_ORGANISATION
*/

WITH organisation AS (
	SELECT
		ID AS ORGANISATION_ID
		,NAME AS ORGANISATION_NAME
		,TERMS_AND_CONDITIONS		
	FROM
		{{ source('Hermes', 'ORGANISATION') }}
)

SELECT
	*
FROM
	organisation