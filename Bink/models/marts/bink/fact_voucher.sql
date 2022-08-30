/* One row per voucher with links loyalty_card_id and loyalty_plan
If there are duplicated it takes the latest loyalty plan by created date */


with vouchers as (
    select * from 
    {{ref('transformed_voucher_keys')}}
)


, loyalty_plan as (
    select * 
    from {{ref('stg_hermes__SCHEME_SCHEME')}}
)

, add_company as (
SELECT  v.voucher_code
       ,v.LOYALTY_CARD_ID
       ,v.LOYALTY_PLAN_ID
       ,v.state
       ,v.date_redeemed
       ,v.date_issued
       ,v.expiry_date
       ,v.EARN_TYPE as voucher_type
       ,case when l.LOYALTY_PLAN_COMPANY = 'ASOS' then 'FALSE'
            else 'TRUE'
        end as Redemption_TRACKED
       ,l.LOYALTY_PLAN_COMPANY
FROM vouchers v
left join loyalty_plan l 
on l.loyalty_plan_id = v.loyalty_plan_id
)



, timings as (
Select 
voucher_code
,LOYALTY_CARD_ID
,LOYALTY_PLAN_ID
,state
,voucher_type
,date_redeemed
,date_issued
,expiry_date
,Redemption_TRACKED
,case when Redemption_TRACKED = 'TRUE' and state in ( 'ISSUED' ,'REDEEMED') and expiry_date >= current_date() -1  then datediff(day, date_issued,coalesce(date_redeemed, current_date()-1)) 
        else null 
        end as time_to_redemption
,case when STATE = 'ISSUED' and Redemption_TRACKED = 'TRUE' and expiry_date >= current_date() -1  then  datediff(day, current_date()-1 ,expiry_date)
        else null 
        end as days_left_on_vouchers
,datediff(day, date_issued ,expiry_date) as days_valid_for
from add_company
)



select * from timings


