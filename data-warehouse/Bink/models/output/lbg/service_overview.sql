/*
Created by:         Anand Bhakta
Created date:       2023-06-30
Last modified by:   
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - service_overview
Parameters:
    source_object       - src__apistats
*/

WITH apistats AS (
    SELECT *
    ,'API' AS TAB
    FROM {{ref('stg_metrics__apistats')}}
    WHERE CHANNEL = 'LLOYDS' 
)

,service AS (
        SELECT *
    ,'SERVICE' AS TAB
    FROM {{ref('stg_metrics__service_management')}}
    WHERE CHANNEL = 'LLOYDS' 
)

,trans AS (
        SELECT *
    ,'TRANS' AS TAB
    FROM {{ref('trans__trans__daily_user_level')}}
    WHERE CHANNEL = 'LLOYDS' 
)

,combine AS (
    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,API_ID
        ,METHOD
        ,PATH
        ,RESPONSE_TIME
        ,STATUS_CODE
        ,NULL AS TICKET_ID
		,NULL AS MI
		,NULL AS SERVICE
		,NULL AS SLA_BREACHED
        ,NULL AS T002__ACTIVE_USERS__USER_LEVEL_DAILY__UID
    FROM
        apistats

    UNION ALL

    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,NULL AS API_ID
        ,NULL AS METHOD
        ,NULL AS PATH
        ,NULL AS RESPONSE_TIME
        ,NULL AS STATUS_CODE
		,TICKET_ID
		,MI
		,SERVICE
		,SLA_BREACHED
        ,NULL AS T002__ACTIVE_USERS__USER_LEVEL_DAILY__UID
	FROM
		service

    UNION ALL

    SELECT
        TAB
        ,DATE
        ,CHANNEL
        ,NULL AS API_ID
        ,NULL AS METHOD
        ,NULL AS PATH
        ,NULL AS RESPONSE_TIME
        ,NULL AS STATUS_CODE
		,NULL AS TICKET_ID
		,NULL AS MI
		,NULL AS SERVICE
		,NULL AS SLA_BREACHED
        ,T002__ACTIVE_USERS__USER_LEVEL_DAILY__UID
	FROM
		trans 
)

select * from combine
