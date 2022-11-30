/*
Created by:         Anand Bhakta
Created date:       2022-11-30
Last modified by:   
Last modified date: 

Description:
    Add brand and alias channel for all events that have a channel field.

Parameters:
    ref_object      - stg_hermes__EVENTS
*/


{{
    config(
        materialized='incremental'
		,unique_key='EVENT_ID'
    )
}}

WITH new_events AS (
    SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	{% if is_incremental() %}
  	WHERE _AIRBYTE_EMITTED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)
,extract_channel AS (
  SELECT 
    JSON
    ,EVENT_TYPE
    ,EVENT_DATE_TIME
    ,_AIRBYTE_AB_ID
    ,_AIRBYTE_EMITTED_AT
    ,_AIRBYTE_NORMALIZED_AT
    ,_AIRBYTE_EVENTS_HASHID
    ,EVENT_ID
    ,JSON:channel::varchar as CHANNEL
  FROM new_events
)

,add_brand AS (
  SELECT
    JSON
    ,EVENT_TYPE
    ,EVENT_DATE_TIME
    ,_AIRBYTE_AB_ID
    ,_AIRBYTE_EMITTED_AT
    ,_AIRBYTE_NORMALIZED_AT
    ,_AIRBYTE_EVENTS_HASHID
    ,EVENT_ID
    ,CASE WHEN CHANNEL IN ('com.bos.api2', 'com.halifax.api2','com.lloyds.api2')  THEN 'LLOYDS'
        WHEN CHANNEL in ('com.barclays.bmb') THEN 'BARCLAYS'
        WHEN CHANNEL in ('com.bink.wallet') THEN 'BINK'
        ELSE CHANNEL
        END AS CHANNEL
    ,CASE WHEN CHANNEL IN ('com.bos.api2', 'com.halifax.api2','com.lloyds.api2')  THEN UPPER(SPLIT_PART(CHANNEL, '.',2))
        WHEN CHANNEL in ('com.barclays.bmb') THEN 'BARCLAYS'
        WHEN CHANNEL in ('com.bink.wallet') THEN 'BINK'
        ELSE CHANNEL
        END AS BRAND
    ,SYSDATE() AS INSERTED_DATE_TIME
  FROM extract_channel
)

SELECT *
FROM add_brand