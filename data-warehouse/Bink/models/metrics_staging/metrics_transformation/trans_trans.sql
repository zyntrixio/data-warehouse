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

   , transforming_deletes AS (
    SELECT date
         // , user_id
         // , external_user_ref
         , channel
         , brand
         , COALESCE(NULLIF(external_user_ref, ''), user_id) AS user_ref
         , transaction_id
         // , provider_slug
         , loyalty_plan_name
         , loyalty_plan_company
         , transaction_date
         , spend_amount
         // , loyalty_id
         , loyalty_card_id
         // , merchant_id
         // , payment_account_id
    FROM trans_events)

   , to_from_dates AS (
    SELECT channel
         , brand
         , user_ref
         , transaction_id
         , loyalty_plan_name
         , loyalty_plan_company
         , transaction_date
         , spend_amount
         , loyalty_card_id
         , date AS from_date
         , COALESCE(
            LEAD(date, 1) OVER (PARTITION BY user_ref, loyalty_plan_name ORDER BY date)
        , CURRENT_TIMESTAMP
        )       AS to_date
    FROM transforming_deletes)

SELECT *
FROM to_from_dates