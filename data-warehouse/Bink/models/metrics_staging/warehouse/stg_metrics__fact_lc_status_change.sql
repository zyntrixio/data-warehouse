WITH source AS (
    SELECT * 
    FROM {{ ref('fact_loyalty_card_status_change_secure') }}
)

,renamed AS (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN_ID
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,FROM_STATUS_ID
        ,FROM_STATUS
        ,TO_STATUS_ID
        ,TO_STATUS
        ,IS_MOST_RECENT
        ,ORIGIN
        ,CHANNEL
        ,BRAND
        ,EXTERNAL_USER_REF
        ,USER_ID
        ,EMAIL_DOMAIN
        ,INSERTED_DATE_TIME
        ,UPDATED_DATE_TIME
    FROM source
)

SELECT *
FROM renamed
