with vouchers as (

Select * from 
{{ref('stg_hermes__VOUCHERS')}}

)

, asos  as (
    select LOYALTY_CARD_ID
,LOYALTY_PLAN_ID
,CREATED
,code
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
,row_number() over(order by created ) as surrogate_rank
    from vouchers
    where code like 'Due:%'
)

, voucher_asos_key  as (
    select LOYALTY_CARD_ID
, LOYALTY_PLAN_ID
,CREATED
,MD5_hex(LOYALTY_CARD_ID || '-'|| surrogate_rank ) code
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
from asos
)

, un_codes as (
    select * from
    voucher_asos_key

    union all 

    select * from VOUCHERS
    where code not like 'Due:%'
)


, de_dupe as (
    select LOYALTY_CARD_ID
,LOYALTY_PLAN_ID
,CREATED
,code as voucher_code
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
,row_number() over(partition by code order by created desc) as voucher_rank
from un_codes
)

select * 
from de_dupe
where voucher_rank = 1