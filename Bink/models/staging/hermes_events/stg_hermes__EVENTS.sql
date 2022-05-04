/*
Created by:         Sam Pibworth
Created date:       2022-05-04
Last modified by:   
Last modified date: 

Description:
    Stages the events table, which is an aggregation of all hermes events including user, payment, and loyalty card information

Parameters:
    source_object      - HERMES_EVENTS.EVENTS
*/

WITH
all_events as (
	SELECT
		*
	FROM
		{{ source('hermes_events', 'events') }}
)

,all_events_select as (
	SELECT
		PARSE_JSON(JSON) AS JSON
		,EVENT_TYPE
		,EVENT_DATE_TIME
	FROM
		all_events
)

SELECT
	*
FROM
	all_events_select