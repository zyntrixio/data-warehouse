/*
Created by:         Sam Pibworth
Created date:       2022-05-04
Last modified by:   Christopher Mitchell
Last modified date: 2023-06-05

Description:
    One row per voucher with links loyalty_card_id and loyalty_plan
If there are duplicated it takes the latest loyalty plan by created date

Parameters:
    ref_object      - transformed_voucher-keys
                    - dim_lc

*/


with vouchers as (
    select * from 
    {{ref('transformed_voucher_keys')}}
)


, loyalty_card as (
    select * 
    from {{ref('dim_loyalty_card')}}
)

, add_company as (

SELECT  v.CREATED
       ,v.loyalty_card_id
       ,lc.LOYALTY_PLAN_COMPANY
       ,lc.LOYALTY_PLAN_NAME
       ,v.state
       ,v.earn_type
       ,v.voucher_code
       ,v.date_redeemed
       ,v.date_issued
       ,v.expiry_date
       ,case when lc.LOYALTY_PLAN_COMPANY = 'ASOS' then 'FALSE'
            when state = 'CANCELLED' then 'FALSE'
            else 'TRUE'
        end as Redemption_TRACKED
FROM vouchers v
left join loyalty_card lc
on v.loyalty_card_id = lc.loyalty_card_id

)



, timings as (
SELECT 
    CREATED
    ,loyalty_card_id
    ,LOYALTY_PLAN_COMPANY
    ,LOYALTY_PLAN_NAME
    ,state
    ,earn_type
    ,voucher_code
    ,Redemption_TRACKED
    ,date_redeemed
    ,date_issued
    ,expiry_date
    ,CASE WHEN Redemption_TRACKED = 'TRUE' and state in ( 'ISSUED' ,'REDEEMED')  then datediff(day, date_issued,coalesce(date_redeemed, current_date())) 
        else NULL 
        end as time_to_redemption
    ,CASE WHEN STATE = 'ISSUED' and Redemption_TRACKED = 'TRUE' and expiry_date >= current_date()  then  datediff(day, current_date() ,expiry_date)
        else NULL 
        END AS days_left_on_vouchers
    ,datediff(day, date_issued ,expiry_date) as days_valid_for
FROM add_company
)

select * from timings
--where current_channel = 'com.barclays.bmb'