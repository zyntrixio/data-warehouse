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

    , dim_date AS (
        SELECT *
        FROM {{ ref('dim_date') }}
        WHERE
            DATE >= (SELECT MIN(date_issued) FROM voucher_trans)
            AND DATE <= CURRENT_DATE()
    )

    , voucher_staging AS (
        SELECT v.date_issued
                , v.channel
                , v.brand
                , v.loyalty_plan_company
                , v.loyalty_plan_name
                , v.voucher_code
                , 'ISSUED' AS STATE
        FROM voucher_trans
            UNION
        SELECT v.date_issued
                , v.channel
                , v.brand
                , v.loyalty_plan_company
                , v.loyalty_plan_name
                , v.voucher_code
                , v.state
        FROM voucher_trans
        WHERE STATE = 'REDEEMED'
            UNION
        SELECT v.date_issued
                , v.channel
                , v.brand
                , v.loyalty_plan_company
                , v.loyalty_plan_name
                , v.voucher_code
                , v.state
        FROM voucher_trans
        WHERE STATE = 'EXPIRED'
    )

   , voucher_metrics AS (
    SELECT d.date
         , v.channel
         , v.brand
         , v.loyalty_plan_company
         , v.loyalty_plan_name
         , COALESCE(COUNT(CASE WHEN state = 'ISSUED' THEN 1 END), 0)   AS daily_issued_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'REDEEMED' THEN 1 END), 0) AS daily_redeemed_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'EXPIRED' THEN 1 END), 0)  AS daily_expired_vouchers
    FROM dim_date d
    LEFT JOIN voucher_staging v 
        ON d.date = DATE(v.date)
    GROUP BY 
        d.date
        , v.channel
        , v.brand
        , v.loyalty_plan_company
        , v.loyalty_plan_name
    HAVING DATE IS NOT NULL
    )

   , voucher_metrics_snap AS (
    SELECT d.date
         , v.channel
         , v.brand
         , v.loyalty_plan_company
         , v.loyalty_plan_name
         , COALESCE(COUNT(CASE WHEN state = 'ISSUED' THEN 1 END), 0)   AS snap_issued_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'REDEEMED' THEN 1 END), 0) AS snap_redeemed_vouchers
         , COALESCE(COUNT(CASE WHEN state = 'EXPIRED' THEN 1 END), 0)  AS snap_expired_vouchers
    FROM dim_date d
    LEFT JOIN voucher_staging v
        ON d.date = DATE(v.date)
    GROUP BY
        d.date
        , v.channel
        , v.brand
        , v.loyalty_plan_company
        , v.loyalty_plan_name
    HAVING DATE IS NOT NULL    
    )

   , combine_all AS (
    SELECT COALESCE(a.date, s.date)               AS DATE
         , COALESCE(a.channel, s.channel)         AS channel
         , COALESCE(a.brand, s.brand)             AS brand
         , COALESCE(a.loyalty_plan_company, s.loyalty_plan_company) AS loyalty_plan_company
         , COALESCE(a.loyalty_plan_name, s.loyalty_plan_name) AS loyalty_plan_name
         , COALESCE(a.daily_issued_vouchers, 0)   AS daily_issued_vouchers
         , COALESCE(a.daily_redeemed_vouchers, 0) AS daily_redeemed_vouchers
         , COALESCE(a.daily_expired_vouchers, 0)  AS daily_expired_vouchers
         , COALESCE(s.snap_issued_vouchers, 0)    AS snap_issued_vouchers
         , COALESCE(s.snap_redeemed_vouchers, 0)  AS snap_redeemed_vouchers
         , COALESCE(s.snap_expired_vouchers, 0)   AS snap_expired_vouchers
    FROM voucher_metrics a
             FULL OUTER JOIN voucher_metrics_snap s
                             ON a.date = s.date AND a.brand = s.brand)

    , rename AS (
        SELECT
            date
            , channel
            , brand
            , daily_expired_vouchers    AS V004__issued_vouchers__daily_channel_brand_retailer__count
            , daily_redeemed_vouchers   AS V005__redeemed_vouchers__daily_channel_brand_retailer__count
            , daily_expired_vouchers    AS V006__expired_vouchers__daily_channel_brand_retailer__count
            , snap_issued_vouchers      AS V001__issued_vouchers__daily_channel_brand_retailer__pit
            , snap_redeemed_vouchers    AS V002__redeemed_vouchers__daily_channel_brand_retailer__pit
            , snap_expired_vouchers     AS V003__expired_vouchers__daily_channel_brand_retailer__pit
        FROM
            combine_all
    )

SELECT *
FROM rename