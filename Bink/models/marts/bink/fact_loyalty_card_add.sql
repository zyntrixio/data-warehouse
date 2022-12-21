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
    FROM {{ref('fact_loyalty_card_add_secure')}}
	{% if is_incremental() %}
  	WHERE UPDATED_DATE_TIME>= (SELECT MAX(UPDATED_DATE_TIME) from {{ this }})
	{% endif %}
)

,lc_select AS (
    SELECT
		EVENT_ID
		,EVENT_DATE_TIME
        ,auth_type
		,EVENT_TYPE
		,LOYALTY_CARD_ID
		,LOYALTY_PLAN
		,LOYALTY_PLAN_NAME
		,IS_MOST_RECENT
		 ,case when MAIN_ANSWER is null then null 
         when MAIN_ANSWER =  '' then null 
         else md5(main_answer)
         end  as main_answer
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