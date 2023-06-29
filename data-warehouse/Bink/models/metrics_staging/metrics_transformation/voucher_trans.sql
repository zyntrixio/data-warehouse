/*
Created by:         Christopher Mitchell
Created date:       2023-05-31
Last modified by:   Christopher Mitchell
Last modified date: 2023-06-06

Description:
    Voucher table, which relates to the transform date into do date and from date for metrics layer

Parameters:
    ref_object      - src__fact_voucher
*/
WITH source AS (
    SELECT *
    FROM {{ ref('src__fact_voucher') }})

   , lc AS (
    SELECT *
    FROM {{ ref('src__fact_lc') }})

   , joins AS (
    SELECT s.created
         , s.loyalty_card_id
         , s.loyalty_plan_company
         , s.loyalty_plan_name
         , s.state
         , s.earn_type
         , s.voucher_code
         , s.redemption_tracked
         , s.date_redeemed
         , s.date_issued
         , s.expiry_date
         , s.time_to_redemption
         , s.days_left_on_vouchers
         , s.days_valid_for
         , lc.event_id
         , lc.event_date_time
         , lc.auth_type
         , lc.event_type
         , lc.is_most_recent
         , lc.main_answer
         , lc.channel
         , lc.brand
         , lc.origin
         , lc.user_id
         , lc.email_domain
         , lc.inserted_date_time
         , lc.updated_date_time
    FROM source s
             LEFT JOIN lc ON lc.loyalty_card_id = s.loyalty_card_id)

   , stage AS (
    SELECT DATE(created)     AS created_date
         , loyalty_card_id
         , loyalty_plan_company
         , loyalty_plan_name
         , state
         , earn_type         AS voucher_type
         , voucher_code
         , redemption_tracked
         , DATE(date_issued) AS date_issued
         , DATE(expiry_date) AS date_expired
         , time_to_redemption
         , days_left_on_vouchers
         , days_valid_for
         , channel
         , brand
    FROM joins)

   , to_date AS (
    SELECT created_date
         , channel
         , brand
         , loyalty_card_id
         , loyalty_plan_company
         , loyalty_plan_name
         , state
         , voucher_type
         , voucher_code
         , redemption_tracked
         , date_issued
         , date_expired
         , date_issued                                                        AS from_date
         , LEAD(date_expired)
                OVER (PARTITION BY loyalty_card_id ORDER BY date_expired ASC) AS to_date
         , time_to_redemption
         , days_left_on_vouchers
         , days_valid_for
    FROM stage)

SELECT *
FROM to_date;