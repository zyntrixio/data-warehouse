with transformed_loyalty_card_channel as (
    select * from {{ref('transformed_loyalty_card_channel')}}
)


, transformed_voucher_keys as  (
    select * from {{ref('transformed_voucher_keys')}}
)

, channel_vocuher as (
SELECT  EVENT_DATE_TIME
       ,c.loyalty_card_id
       ,channel
       ,user_id
       ,LOYALTY_PLAN_ID
       ,CREATED
       ,voucher_code
       ,barcode_type
       ,body_text
       ,burn_currency
       ,burn_prefix
       ,burn_suffix
       ,burn_type
       ,burn_value
       ,earn_currency
       ,earn_prefix
       ,earn_suffix
       ,earn_target_value
       ,earn_type
       ,earn_value
       ,headline
       ,state
       ,subtext
       ,terms_and_conditions_url
       ,date_redeemed
       ,date_issued
       ,expiry_date
--row_number()over(partition by voucher_code ORDER BY EVENT_DATE_TIME ) AS create_rank
FROM transformed_voucher_keys k
INNER JOIN transformed_loyalty_card_channel c
ON c.loyalty_card_id = k.loyalty_card_id AND k.EXPIRY_DATE >= EVENT_DATE_TIME
)


, rank_times as (
SELECT  EVENT_DATE_TIME
       ,loyalty_card_id
       ,state
       ,earn_type
       ,channel
       ,user_id
       ,voucher_code
       ,date_redeemed
       ,date_issued
       ,expiry_date
       ,row_number()over(partition by voucher_code ORDER BY EVENT_DATE_TIME )                                                           AS create_rank
       ,CASE WHEN DATEDIFF(minute,event_date_time,date_issued) < 0 THEN null  ELSE DATEDIFF(minute,event_date_time,date_issued) END     AS issue_time
       ,CASE WHEN DATEDIFF(minute,event_date_time,date_redeemed) < 0 THEN null  ELSE DATEDIFF(minute,event_date_time,date_redeemed) END AS redeem_time
FROM channel_vocuher
ORDER BY voucher_code , create_rank)

,redeem_ranks as (

SELECT  EVENT_DATE_TIME
       ,loyalty_card_id
       ,channel
       ,user_id
       ,state
       ,earn_type
       ,voucher_code
       ,date_redeemed
       ,date_issued
       ,expiry_date
       ,issue_time
       ,row_number()over(partition by voucher_code ORDER BY issue_time )  AS issue_rank
       ,redeem_time
       ,row_number()over(partition by voucher_code ORDER BY redeem_time ) AS redeem_rank
FROM rank_times
  
  )
  
  , issus_redeem as (
  
SELECT  EVENT_DATE_TIME
       ,loyalty_card_id
       ,channel
       ,user_id
       ,state
       ,earn_type
       ,voucher_code
       ,date_redeemed
       ,date_issued
       ,expiry_date
       ,issue_time
       ,issue_rank
       ,CASE WHEN issue_rank = 1 AND issue_time is not null THEN 'TRUE'  ELSE null END   AS issued
       ,redeem_time
       ,redeem_rank
       ,CASE WHEN redeem_rank = 1 AND redeem_time is not null THEN 'TRUE'  ELSE null END AS redemed
FROM redeem_ranks
    
    )
    

    , final as(
SELECT  p1.EVENT_DATE_TIME
       ,p1.loyalty_card_id
       ,p1.channel AS current_channel
       ,p1.user_id
       ,p1.state
       ,p1.earn_type
       ,p1.voucher_code
       ,p1.date_redeemed
       ,p1.date_issued
       ,p1.expiry_date
       ,i.issued
       ,i.channel  AS issued_channel
       ,r.redemed
       ,r.channel  AS redeemed_channel
       ,row_number()over(partition by p1.channel,p1.voucher_code ORDER BY p1.EVENT_DATE_TIME desc ) current_channel_rank
FROM issus_redeem p1
LEFT JOIN issus_redeem i
ON i.voucher_code = p1.voucher_code AND i.issued = 'TRUE'
--and i.issued != p1.issued
LEFT JOIN issus_redeem r
ON r.voucher_code = p1.voucher_code AND r.redemed = 'TRUE'
--and r.redemed != p1.redemed
qualify current_channel_rank = 1

    )


    select * from final