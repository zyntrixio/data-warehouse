/*
Created by:         Sam Pibworth
Created date:       2022-06-15
Last modified by:   
Last modified date: 

Description:
	Fact payment account with reduced columns

Parameters:
    ref_object      - fact_payment_account_secure
*/

{{
    config(
        materialized='incremental'
		,unique_key='EVENT_ID'
		,merge_update_columns = ['IS_MOST_RECENT', 'UPDATED_DATE_TIME']
    )
}}

WITH
pa AS (
    SELECT * 
    FROM {{ref('fact_payment_account_secure')}}
	{% if is_incremental() %}
  	WHERE UPDATED_DATE_TIME>= (SELECT MAX(UPDATED_DATE_TIME) from {{ this }})
	{% endif %}
)

,pa_select AS (
    SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,EVENT_TYPE
		,PAYMENT_ACCOUNT_ID
		,IS_MOST_RECENT
		,ORIGIN
		,CHANNEL
		,BRAND
		,USER_ID
		// ,EXTERNAL_USER_REF
		// ,EXPIRY_DATE
		,TOKEN
		,STATUS_ID
		,STATUS
		,EMAIL_DOMAIN
		,INSERTED_DATE_TIME
		,UPDATED_DATE_TIME
    FROM
        pa
)


SELECT *
FROM pa_select