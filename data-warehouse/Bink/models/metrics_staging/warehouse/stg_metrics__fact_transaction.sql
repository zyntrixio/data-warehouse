WITH source AS (
    SELECT * 
    FROM {{ ref('fact_transaction_secure') }}
)

,renamed AS (
    SELECT
        // EVENT_ID
        EVENT_DATE_TIME AS DATE
        ,USER_ID
        ,EXTERNAL_USER_REF
        ,CHANNEL
        ,BRAND
        ,TRANSACTION_ID
        ,PROVIDER_SLUG
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,TRANSACTION_DATE
        ,SPEND_AMOUNT
        // ,SPEND_CURRENCY
        ,LOYALTY_ID
        ,LOYALTY_CARD_ID
        ,MERCHANT_ID
        ,PAYMENT_ACCOUNT_ID
        // ,SETTLEMENT_KEY
        // ,INSERTED_DATE_TIME
        // ,UPDATED_DATE_TIME
    FROM source
    WHERE USER_ID IN (SELECT USER_ID FROM {{ref('stg_metrics__fact_user')}}) -- required for creating consistent data sources
)

SELECT *
FROM renamed
