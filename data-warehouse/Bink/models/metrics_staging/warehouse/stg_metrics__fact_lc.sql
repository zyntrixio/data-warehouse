WITH source AS (
    SELECT * 
    FROM {{ ref('fact_loyalty_card_secure') }}
)

,renamed AS (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,AUTH_TYPE
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,IS_MOST_RECENT
        ,CHANNEL
        ,ORIGIN
        ,BRAND
        ,USER_ID
        ,EXTERNAL_USER_REF
        ,EMAIL_DOMAIN
        ,CONSENT_SLUG
        ,CONSENT_RESPONSE        
        ,INSERTED_DATE_TIME
        ,UPDATED_DATE_TIME
    FROM source
)

SELECT *
FROM renamed
