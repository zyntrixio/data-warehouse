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
*/

{{
    config(
		alias='fact_pll_link_status_change'
        ,materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}


WITH pll AS (
    SELECT *
    FROM {{ref('fact_pll_link_status_change_secure')}}
	{% if is_incremental() %}
  	WHERE UPDATED_DATE_TIME>= (SELECT MAX(UPDATED_DATE_TIME) from {{ this }})
	{% endif %}
)

,pll_select AS (
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
		//,EXTERNAL_USER_REF
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY LOYALTY_CARD_ID, PAYMENT_ACCOUNT_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,INSERTED_DATE_TIME
		,SYSDATE() AS UPDATED_DATE_TIME
	FROM
		pll
)

select * from pll_select
