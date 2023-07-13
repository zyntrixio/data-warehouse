WITH
source as (
	SELECT
		*
	FROM
		{{ ref('fact_pll_link_status_change_secure') }}
)

,renamed as (
	SELECT
		EVENT_ID
		,EVENT_DATE_TIME
		,LOYALTY_CARD_ID
        //,LOYALTY_PLAN_ID
        ,LOYALTY_PLAN_COMPANY
        ,LOYALTY_PLAN_NAME
        ,PAYMENT_ACCOUNT_ID
		//,FROM_STATUS_ID
        ,FROM_STATUS
		//,TO_STATUS_ID
        ,TO_STATUS
        ,CHANNEL
		,BRAND
		//,ORIGIN
		,USER_ID
		,EXTERNAL_USER_REF
		//,IS_MOST_RECENT
		//,INSERTED_DATE_TIME
		//,UPDATED_DATE_TIME
	FROM
		source
)


SELECT
	*
FROM
	renamed
