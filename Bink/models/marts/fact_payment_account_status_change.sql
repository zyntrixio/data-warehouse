/*
Created by:         Sam Pibworth
Created date:       2022-05-05
Last modified by:   
Last modified date: 

Description:
    Extracts payment_account_status_change from the events table

Parameters:
    ref_object      - stg_hermes__events
*/


WITH

payment_events AS (
	SELECT *
	FROM {{ ref('stg_hermes__EVENTS')}}
	WHERE EVENT_TYPE = 'payment.account.status.change'
	{% if is_incremental() %}
  	AND _AIRBYTE_NORMALIZED_AT >= (SELECT MAX(INSERTED_DATE_TIME) from {{ this }})
	{% endif %}
)

,payment_account_status_lookup AS (
	SELECT *
	FROM {{ ref ('stg_lookup__PAYMENT_ACCOUNT_STATUS') }}
)

,payment_events_unpack AS (
	SELECT
		EVENT_ID
		,EVENT_TYPE
		,EVENT_DATE_TIME
		,JSON:origin::varchar as ORIGIN
		,JSON:channel::varchar as CHANNEL
		,JSON:external_user_ref::varchar as EXTERNAL_USER_REF
		,JSON:internal_user_ref::varchar as USER_ID
		,JSON:email::varchar as EMAIL
		,JSON:payment_account_id::varchar as PAYMENT_ACCOUNT_ID
		,JSON:expiry_date::varchar as EXPIRY_DATE
		,JSON:token::varchar as TOKEN
		,JSON:from_status::integer as FROM_STATUS_ID
		,JSON:to_status::integer as TO_STATUS_ID
	FROM payment_events
)

,payment_events_join_status AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,PAYMENT_ACCOUNT_ID
		,ORIGIN
		,CHANNEL
		,USER_ID
		,EXTERNAL_USER_REF
		,EXPIRY_DATE
		,TOKEN
		,FROM_STATUS_ID
		,s_from.STATUS AS FROM_STATUS
		,TO_STATUS_ID
		,s_to.STATUS AS TO_STATUS
		,EMAIL
	FROM
		payment_events_unpack
	LEFT JOIN payment_account_status_lookup s_from
		ON FROM_STATUS_ID = s_from.ID
	LEFT JOIN payment_account_status_lookup s_to
		ON TO_STATUS_ID = s_to.ID
)

,payment_events_select AS (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,PAYMENT_ACCOUNT_ID
		,CASE WHEN
			(EVENT_DATE_TIME = MAX(EVENT_DATE_TIME) OVER (PARTITION BY USER_ID))
			THEN TRUE
			ELSE FALSE
			END AS IS_MOST_RECENT
		,FROM_STATUS_ID
		,FROM_STATUS
		,TO_STATUS_ID
		,TO_STATUS
		,ORIGIN
		,CHANNEL
		,USER_ID
		,EXTERNAL_USER_REF
		,SPLIT_PART(EXPIRY_DATE,'/',1)::integer AS EXPIRY_MONTH
		,CASE WHEN SPLIT_PART(EXPIRY_DATE,'/',2)::integer >= 2000
			THEN SPLIT_PART(EXPIRY_DATE,'/',2)::integer
			ELSE SPLIT_PART(EXPIRY_DATE,'/',2)::integer + 2000
			END AS EXPIRY_YEAR
		,CONCAT(EXPIRY_YEAR, '-', EXPIRY_MONTH) as EXPIRY_YEAR_MONTH
		,TOKEN
		,LOWER(EMAIL) AS EMAIL
		,SPLIT_PART(EMAIL,'@',2) AS EMAIL_DOMAIN
		,CURRENT_TIMESTAMP() AS INSERTED_DATE_TIME
	FROM payment_events_join_status
)

SELECT
	*
FROM
	payment_events_select
	