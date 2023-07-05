/*
Created by:         Christopher Mitchell
Created date:       2023-07-05
Last modified by:    
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - vouchers_overview
Parameters:
    source_object       - voucher__daily_channel_brand_retailer
                        - voucher__times__voucher_level_channel_brand
*/

WITH voucher_daily AS (
    SELECT *
         , 'DAILY' AS tab
    FROM {{ ref('voucher__daily_channel_brand_retailer') }}
    WHERE channel = 'LLOYDS'
      AND loyalty_plan_company NOT IN ('Bink Sweet Shop', 'Loyalteas'))

   , voucher_times AS (
    SELECT *
         , 'TIMES' AS tab
    FROM {{ ref('voucher__times__voucher_level_channel_brand') }}
    WHERE channel = 'LLOYDS'
      AND loyalty_plan_company NOT IN ('Bink Sweet Shop', 'Loyalteas'))

   , union_all AS (
    SELECT tab
         , date
         , channel
         , brand
         , loyalty_plan_company
         , loyalty_plan_name
         , v004__issued_vouchers__daily_channel_brand_retailer__count
         , v005__redeemed_vouchers__daily_channel_brand_retailer__count
         , v006__expired_vouchers__daily_channel_brand_retailer__count
         , v001__issued_vouchers__daily_channel_brand_retailer__cdsum_voucher
         , v002__redeemed_vouchers__daily_channel_brand_retailer__cdsum_voucher
         , v003__expired_vouchers__daily_channel_brand_retailer__cdsum_voucher
         , NULL AS state
         , NULL AS earn_type
         , NULL AS voucher_code
         , NULL AS redemption_tracked
         , NULL AS date_redeemed
         , NULL AS date_issued
         , NULL AS expiry_date
         , NULL AS v007__time_to_redemption__voucher_level__sum
         , NULL AS v008__days_left_on_voucher__voucher_level__sum
    FROM voucher_daily
    UNION ALL
    SELECT tab
         , NULL AS date
         , channel
         , brand
         , loyalty_plan_company
         , loyalty_plan_name
         , NULL AS v004__issued_vouchers__daily_channel_brand_retailer__count
         , NULL AS v005__redeemed_vouchers__daily_channel_brand_retailer__count
         , NULL AS v006__expired_vouchers__daily_channel_brand_retailer__count
         , NULL AS v001__issued_vouchers__daily_channel_brand_retailer__cdsum_voucher
         , NULL AS v002__redeemed_vouchers__daily_channel_brand_retailer__cdsum_voucher
         , NULL AS v003__expired_vouchers__daily_channel_brand_retailer__cdsum_voucher
         , state
         , earn_type
         , voucher_code
         , redemption_tracked
         , date_redeemed
         , date_issued
         , expiry_date
         , v007__time_to_redemption__voucher_level__sum
         , v008__days_left_on_voucher__voucher_level__sum
    FROM voucher_times)

SELECT *
FROM union_all