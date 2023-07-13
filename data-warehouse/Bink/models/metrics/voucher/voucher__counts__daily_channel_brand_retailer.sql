/*
Created by:         Christopher Mitchell 
Created date:       2023-07-04
Last modified by:   
Last modified date: 

Description:
    todo
Parameters:
    source_object       - voucher_trans
                        - dim_date
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
        SELECT v.date_issued AS DATE
                , v.channel
                , v.brand
                , v.loyalty_plan_company
                , v.loyalty_plan_name
                , v.voucher_code
                , 'ISSUED' AS STATE
        FROM voucher_trans v
            UNION
        SELECT v.date_redeemed AS DATE
                , v.channel
                , v.brand
                , v.loyalty_plan_company
                , v.loyalty_plan_name
                , v.voucher_code
                , v.state
        FROM voucher_trans v
        WHERE STATE = 'REDEEMED'
            UNION
        SELECT v.expiry_date AS DATE
                , v.channel
                , v.brand
                , v.loyalty_plan_company
                , v.loyalty_plan_name
                , v.voucher_code
                , v.state
        FROM voucher_trans  v
        WHERE STATE = 'EXPIRED'
    )

   , voucher_metrics AS (
    SELECT d.date
         , v.channel
         , v.brand
         , v.loyalty_plan_company
         , v.loyalty_plan_name
         , COALESCE(COUNT(DISTINCT CASE WHEN state = 'ISSUED' THEN VOUCHER_CODE END), 0)   AS daily_issued_vouchers
         , COALESCE(COUNT(DISTINCT CASE WHEN state = 'REDEEMED' THEN VOUCHER_CODE END), 0) AS daily_redeemed_vouchers
         , COALESCE(COUNT(DISTINCT CASE WHEN state = 'EXPIRED' THEN VOUCHER_CODE END), 0)  AS daily_expired_vouchers
    FROM dim_date d
    LEFT JOIN voucher_staging v 
        ON d.date = DATE(v.date)
    GROUP BY 
        d.date
        , v.channel
        , v.brand
        , v.loyalty_plan_company
        , v.loyalty_plan_name
    )

    , rename AS (
        SELECT
            date
            , channel
            , brand
            , loyalty_plan_company
            , loyalty_plan_name
            , daily_issued_vouchers                                                                         AS V004__issued_vouchers__daily_channel_brand_retailer__count
            , daily_redeemed_vouchers                                                                       AS V005__redeemed_vouchers__daily_channel_brand_retailer__count
            , daily_expired_vouchers                                                                        AS V006__expired_vouchers__daily_channel_brand_retailer__count
            ,SUM(daily_issued_vouchers) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND ORDER BY DATE ASC)   AS V001__issued_vouchers__daily_channel_brand_retailer__cdsum_voucher
            ,SUM(daily_redeemed_vouchers) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND ORDER BY DATE ASC) AS V002__redeemed_vouchers__daily_channel_brand_retailer__cdsum_voucher
            ,SUM(daily_expired_vouchers) OVER (PARTITION BY LOYALTY_PLAN_COMPANY, BRAND ORDER BY DATE ASC)  AS V003__expired_vouchers__daily_channel_brand_retailer__cdsum_voucher
        FROM
            voucher_metrics
        WHERE CHANNEL IS NOT NULL
    )

SELECT *
FROM rename
