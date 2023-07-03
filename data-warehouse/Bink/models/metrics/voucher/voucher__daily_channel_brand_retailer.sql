{{ config(
  enabled= false
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('voucher_trans') }})

    , stage AS (
        SELECT
            LOYALTY_CARD_ID
            , USER_ID
            , CHANNEL
            , state
            , earn_type
            , voucher_code
            , REDEMPTION_TRACKED
            , DATE_REDEEMED
            , DATE_ISSUED
            , EXPIRY_DATE
            , TIME_TO_REDEMPTION
            , DAYS_VALID_FOR
            , days_left_on_vouchers
            , loyalty_plan_company
            , loyalty_plan_name
       FROM source
    )

    , metrics AS (
        SELECT 
    )