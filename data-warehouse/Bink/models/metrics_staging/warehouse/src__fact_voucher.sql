WITH source AS (

    SELECT * FROM {{ref('fact_voucher')}}

),

renamed AS (

    SELECT
        created
        , loyalty_card_id
        , state
        , earn_type
        , voucher_code
        , redemption_tracked
        , date_redeemed
        , date_issued
        , expiry_date
        , time_to_redemption
        , days_left_on_vouchers
        , days_valid_for
    FROM source

)

SELECT * FROM renamed