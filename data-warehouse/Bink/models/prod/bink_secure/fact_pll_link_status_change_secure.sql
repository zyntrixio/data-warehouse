/*
Created by:         Anand Bhakta
Created date:       2023-07-04
Last modified by:   
Last modified date: 

Description:
    Fact table for loyalty card and payment card pll link status
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all events which require updating, finally calculating is_most_recent
	flag, and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
					- stg_hermes__CLIENT_APPLICATION
					- stg_hermes__SCHEME_SCHEMEACCOUNT
					- stg_hermes__SCHEME_SCHEME

*/


{{
    config(
		alias='fact_pll_link_status_change'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
status_change_events AS (
	SELECT *
	FROM {{ ref('transformed_hermes_events')}}
	WHERE EVENT_TYPE = 'pll_link.statuschange'
	{% if is_incremental() %}
  	AND _AIRBYTE_EMITTED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)

,dim_channel AS (
    SELECT *
    FROM {{ ref('stg_hermes__CLIENT_APPLICATION')}}
)

,dim_loyalty AS (
    SELECT *
    FROM {{ ref('stg_hermes__SCHEME_SCHEMEACCOUNT')}}
)

,dim_loyalty_plan AS (
    SELECT *
    FROM {{ ref('stg_hermes__SCHEME_SCHEME')}}
)

,status_change_events_unpack AS (
	SELECT
		EVENT_TYPE
		,EVENT_DATE_TIME
		,EVENT_ID
		,c.CHANNEL_NAME
        ,JSON:origin::varchar as ORIGIN
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:scheme_account_id::varchar as LOYALTY_CARD_ID
        ,JSON:payment_account_id::varchar as PAYMENT_ACCOUNT_ID
		,JSON:from_state::int as FROM_STATUS_ID
		,JSON:to_state::int as TO_STATUS_ID
	FROM status_change_events s
    LEFT JOIN dim_channel c ON c.CHANNEL_ID = s.CHANNEL
)

,status_change_events_case AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,sce.LOYALTY_CARD_ID
        ,p.LOYALTY_PLAN_ID
        ,p.LOYALTY_PLAN_COMPANY
        ,p.LOYALTY_PLAN_NAME
        ,PAYMENT_ACCOUNT_ID
		-- ,LOYALTY_PLAN_ID
		,FROM_STATUS_ID
		,TO_STATUS_ID
		,CASE FROM_STATUS_ID
            WHEN 0 THEN 'PENDING'
            WHEN 1 THEN 'ACTIVE'
            WHEN 2 THEN 'INACTIVE'
            ELSE NULL
            END AS FROM_STATUS
		,CASE TO_STATUS_ID
            WHEN 0 THEN 'PENDING'
            WHEN 1 THEN 'ACTIVE'
            WHEN 2 THEN 'INACTIVE'
            ELSE NULL
            END AS TO_STATUS
        ,CASE 
            WHEN CHANNEL_NAME IN ('Bank of Scotland', 'Lloyds', 'Halifax') THEN 'LLOYDS'
            WHEN CHANNEL_NAME = 'Barclays Mobile Banking' THEN 'BARCLAYS'
            ELSE UPPER(CHANNEL_NAME)
            END AS CHANNEL
		,CASE CHANNEL_NAME
            WHEN 'Bank of Scotland' THEN 'BOS'
            WHEN 'Barclays Mobile Banking' THEN 'BARCLAYS'
            ELSE UPPER(CHANNEL_NAME)
            END AS BRAND
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,NULL AS IS_MOST_RECENT
        ,SYSDATE() AS INSERTED_DATE_TIME
		,NULL AS UPDATED_DATE_TIME
	FROM status_change_events_unpack sce
    LEFT JOIN dim_loyalty l on l.LOYALTY_CARD_ID = sce.LOYALTY_CARD_ID
    LEFT JOIN dim_loyalty_plan p on p.LOYALTY_PLAN_ID = l.LOYALTY_PLAN_ID
    QUALIFY NOT(EQUAL_NULL(FROM_STATUS_ID, LAG(FROM_STATUS_ID, 1) OVER (PARTITION BY sce.LOYALTY_CARD_ID, PAYMENT_ACCOUNT_ID  ORDER BY EVENT_DATE_TIME ASC)) 
            AND
            EQUAL_NULL(TO_STATUS_ID, LAG(TO_STATUS_ID, 1) OVER (PARTITION BY sce.LOYALTY_CARD_ID, PAYMENT_ACCOUNT_ID  ORDER BY EVENT_DATE_TIME ASC) )) -- REMOVING DUPLICATES
)

,union_old_lc_records AS (
	SELECT *
	FROM status_change_events_case
	{% if is_incremental() %}
	UNION
	SELECT *
	FROM {{ this }}
	WHERE (LOYALTY_CARD_ID, PAYMENT_ACCOUNT_ID) IN (
		SELECT LOYALTY_CARD_ID, PAYMENT_ACCOUNT_ID
		FROM status_change_events_case
	)
	{% endif %}
)

,alter_is_most_recent_flag AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
        ,LOYALTY_PLAN_ID
        ,LOYALTY_PLAN_COMPANY
        ,LOYALTY_PLAN_NAME
        ,PAYMENT_ACCOUNT_ID
		,FROM_STATUS_ID
        ,FROM_STATUS
		,TO_STATUS_ID
        ,TO_STATUS
        ,CHANNEL
		,BRAND
		,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY LOYALTY_CARD_ID, PAYMENT_ACCOUNT_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,INSERTED_DATE_TIME
		,SYSDATE() AS UPDATED_DATE_TIME
	FROM
		union_old_lc_records
)

SELECT
	*
FROM
	alter_is_most_recent_flag
