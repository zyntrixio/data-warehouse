/*
Created by:         Christopher Mitchell 
Created date:       2023-07-04
Last modified by:   
Last modified date: 

Description:
    todo
Parameters:
    source_object       - voucher_trans
*/

WITH voucher_trans AS (
    SELECT *
    FROM {{ ref('voucher_trans') }})

   , voucher_staging AS (
    SELECT date_issued
         , channel
         , brand
         , loyalty_plan_company
         , COALESCE(COUNT(CASE WHEN state = 'ISSUED' THEN 1 END), 0)   AS daily_issued_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'REDEEMED' THEN 1 END), 0) AS daily_redeemed_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'EXPIRED' THEN 1 END), 0)  AS daily_expired_vouchers
    FROM voucher_trans
    GROUP BY date_issued, channel, brand, loyalty_plan_company)

   , voucher_staging_snap AS (
    SELECT date_issued
         , channel
         , brand
         , loyalty_plan_company
         , COALESCE(COUNT(CASE WHEN state = 'ISSUED' THEN 1 END), 0)   AS snap_issued_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'REDEEMED' THEN 1 END), 0) AS snap_redeemed_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'EXPIRED' THEN 1 END), 0)  AS snap_expired_vouchers
    FROM voucher_trans
    GROUP BY date_issued, channel, brand, loyalty_plan_company)

   , combine_all AS (
    SELECT COALESCE(a.date_issued, s.date_issued) AS date_issued
         , COALESCE(a.channel, s.channel)         AS channel
         , COALESCE(a.brand, s.brand)             AS brand
         , COALESCE(a.daily_issued_vouchers, 0)   AS daily_issued_vouchers
         , COALESCE(a.daily_redeemed_vouchers, 0) AS daily_redeemed_vouchers
         , COALESCE(a.daily_expired_vouchers, 0)  AS daily_expired_vouchers
         , COALESCE(s.snap_issued_vouchers, 0)    AS snap_issued_vouchers
         , COALESCE(s.snap_redeemed_vouchers, 0)  AS snap_redeemed_vouchers
         , COALESCE(s.snap_expired_vouchers, 0)   AS snap_expired_vouchers
    FROM voucher_staging a
             FULL OUTER JOIN voucher_staging_snap s
                             ON a.date_issued = s.date_issued AND a.brand = s.brand)

    , rename AS (
        SELECT
            date_issued
            , channel
            , brand
            , daily_expired_vouchers    AS V004__period_issued_vouchers
            , daily_redeemed_vouchers   AS V005__period_redeemed_vouchers
            , daily_expired_vouchers    AS V006__period_expired_vouchers
            , snap_issued_vouchers      AS V001__total_issued_vouchers
            , snap_redeemed_vouchers    AS V002__total_redeemed_vouchers
            , snap_expired_vouchers     AS V003__total_expired_vouchers
        FROM
            combine_all
    )

SELECT *
FROM rename