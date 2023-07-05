/*
Created by:         Christopher Mitchell
Created date:       2023-06-30
Last modified by:    
Last modified date: 

Description:
    Voucher table, which relates to the transform date into do date and from date for metrics layer

Parameters:
    ref_object      - src__fact_voucher
*/

with FACT_VOUCHER AS (
    SELECT * 
    FROM {{ref('src__fact_voucher')}}

)


, LC_TRANS AS (
    SELECT * 
    FROM {{ref('lc_trans')}}
    WHERE EVENT_TYPE = 'SUCCESS'
)

,issued as (
SELECT  v.LOYALTY_CARD_ID
       , v.state
       , v.earn_type
       , v.voucher_code
       , v.REDEMPTION_TRACKED
       , v.DATE_REDEEMED
       , v.DATE_ISSUED
       , v.EXPIRY_DATE
       , v.TIME_TO_REDEMPTION
       , v.DAYS_VALID_FOR
       , v.days_left_on_vouchers
       , l.user_id
       , l.channel
       , l.brand
       , l.loyalty_plan_company
       , l.loyalty_plan_name
FROM FACT_VOUCHER v
INNER JOIN LC_TRANS l
ON v.date_issued BETWEEN l.from_date AND l.to_date AND v.loyalty_card_id = l.loyalty_card_id
)



, redeem as (
SELECT  v.LOYALTY_CARD_ID
       , v.state
       , v.earn_type
       , v.voucher_code
       , v.REDEMPTION_TRACKED
       , v.DATE_REDEEMED
       , v.DATE_ISSUED
       , v.EXPIRY_DATE
       , v.TIME_TO_REDEMPTION
       , v.DAYS_VALID_FOR
       , v.days_left_on_vouchers
       , l.user_id
       , l.channel
       , l.brand
       , l.loyalty_plan_company
       , l.loyalty_plan_name
FROM FACT_VOUCHER v
INNER JOIN LC_TRANS l
ON v.date_redeemed BETWEEN l.from_date AND l.to_date AND v.loyalty_card_id = l.loyalty_card_id
  
  )

  , final as (
  
SELECT  coalesce(i.LOYALTY_CARD_ID,r.LOYALTY_CARD_ID)                                                                                                AS LOYALTY_CARD_ID
       , coalesce(i.USER_ID,r.USER_ID)                                                                                                               AS USER_ID
       , coalesce(i.CHANNEL,r.CHANNEL)                                                                                                               AS CHANNEL
       , coalesce(i.BRAND, r.BRAND)                                                                                                                  AS BRAND
       , coalesce(i.loyalty_plan_company, r.loyalty_plan_company)                                                                                    AS loyalty_plan_company
       , coalesce(i.loyalty_plan_name, r.loyalty_plan_name)                                                                                          AS loyalty_plan_name
       , coalesce(i.state,r.state)                                                                                                                   AS state
       , coalesce(i.earn_type,r.earn_type)                                                                                                           AS earn_type
       , coalesce(i.voucher_code,i.voucher_code)                                                                                                     AS voucher_code
       , coalesce(i.REDEMPTION_TRACKED,r.REDEMPTION_TRACKED)                                                                                         AS REDEMPTION_TRACKED
       , coalesce(r.DATE_REDEEMED,null)                                                                                                              AS DATE_REDEEMED
       , coalesce(i.DATE_ISSUED,null)                                                                                                                AS DATE_ISSUED
       , coalesce(i.EXPIRY_DATE,r.EXPIRY_DATE)                                                                                                       AS EXPIRY_DATE
       , CASE WHEN i.DATE_ISSUED IS not null AND r.date_redeemed IS not null THEN coalesce(i.TIME_TO_REDEMPTION,r.TIME_TO_REDEMPTION)  ELSE null END AS TIME_TO_REDEMPTION
       , CASE WHEN i.DATE_ISSUED IS not null AND r.date_redeemed IS not null THEN coalesce(i.DAYS_VALID_FOR,r.DAYS_VALID_FOR)  ELSE null END         AS DAYS_VALID_FOR
       , coalesce(i.days_left_on_vouchers,r.days_left_on_vouchers)                                                                                   AS days_left_on_vouchers
       
FROM issued i
FULL OUTER JOIN redeem r
ON r.LOYALTY_CARD_ID = i.LOYALTY_CARD_ID AND r.voucher_code = i.voucher_code AND r.user_id = i.user_id AND r.channel = r.channel
ORDER BY i.voucher_code

  )

  select *
  from final
