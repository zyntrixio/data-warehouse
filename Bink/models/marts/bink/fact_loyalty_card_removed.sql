/*
Created by:         Sam Pibworth
Created date:       2022-06-15
Last modified by:   
Last modified date: 

Description:
	Fact LC removed with reduced columns

Parameters:
    ref_object      - fact_loyalty_card_removed_secure
*/

{{
    config(
        materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
lc AS (
    SELECT * 
    FROM {{ref('fact_loyalty_card_removed_secure')}}
	{% if is_incremental() %}
  	WHERE UPDATED_DATE_TIME>= (SELECT MAX(UPDATED_DATE_TIME) from {{ this }})
	{% endif %}
)

,lc_select AS (
    SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,LOYALTY_PLAN_NAME
		,IS_MOST_RECENT
		// ,MAIN_ANSWER
		,CHANNEL
		,BRAND
		,ORIGIN
		,USER_ID
		// ,EXTERNAL_USER_REF
		// ,EMAIL
		,EMAIL_DOMAIN
		,INSERTED_DATE_TIME
		,UPDATED_DATE_TIME
    FROM
        lc
)


SELECT *
FROM lc_select