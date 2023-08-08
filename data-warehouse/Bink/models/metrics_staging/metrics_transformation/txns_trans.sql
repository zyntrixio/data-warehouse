/*
Created by:         Christopher Mitchell
Created date:       2023-07-17
Last modified by:    
Last modified date: 

Description:
    User table, which relates to the transform date into do date and from date for metrics layer

Parameters:
    ref_object      - stg_metrics__fact_transaction
*/

WITH trans_events AS (
    SELECT *
    FROM {{ ref('stg_metrics__fact_transaction') }})

   , transforming_refs AS (
    SELECT date
         , user_id
         // , external_user_ref
         , channel
         , brand
         , COALESCE(NULLIF(external_user_ref, ''), user_id) AS user_ref
         , transaction_id
         // , provider_slug
         ,DUPLICATE_TRANSACTION
         ,FEED_TYPE
         , loyalty_plan_name
         , loyalty_plan_company
         , transaction_date
         , spend_amount
         // , loyalty_id
         , loyalty_card_id
         // , merchant_id
         // , payment_account_id
    FROM trans_events)

    , txn_flag AS (
    SELECT *
         ,CASE 
            WHEN DUPLICATE_TRANSACTION THEN 'DUPLICATE'
            WHEN loyalty_plan_company = 'Viator' AND spend_amount = 1 OR spend_amount = -1 THEN 'BNPL'
            WHEN spend_amount > 0 THEN 'TXNS'
            WHEN spend_amount < 0 THEN 'REFUND'
            ELSE 'OTHER'
        END AS status
    FROM transforming_refs)

SELECT *
FROM txn_flag
