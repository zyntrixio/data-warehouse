/*
Created by:         Sam Pibworth
Created date:       2022-06-15
Last modified by:   
Last modified date: 

Description:
	Fact LC status change with reduced columns

Parameters:
    ref_object      - fact_loyalty_card_status_change_secure
*/

{{
    config(
        materialized='incremental'
		,unique_key='EVENT_ID'
    )
}}

WITH
lc AS (
    SELECT * 
    FROM {{ref('fact_loyalty_card_status_change_secure')}}
	{% if is_incremental() %}
  	WHERE INSERTED_DATE_TIME >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)

,lc_select AS (
    SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN_ID
		,LOYALTY_PLAN_NAME
		,FROM_STATUS_ID
		,FROM_STATUS
		,TO_STATUS_ID
		,TO_STATUS
		// ,MAIN_ANSWER
		,ORIGIN
		,CHANNEL
		,USER_ID
		// ,EXTERNAL_USER_REF
		// ,EMAIL
		,EMAIL_DOMAIN
		,INSERTED_DATE_TIME
    FROM
        lc
)


SELECT *
FROM lc_select