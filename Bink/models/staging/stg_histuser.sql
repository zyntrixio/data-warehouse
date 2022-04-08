/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   
Last modified date: 

Description:
    Stages the history user table, containing data about changes to the user entities

Parameters:
    source_object      - HERMES.HISTORY_HISTORICALCUSTOMUSER
*/

WITH hist_users AS (
	SELECT
		*
	FROM
		{{ source('Bink', 'HISTORY_USER') }}
)

SELECT
	*
FROM
	hist_users
	