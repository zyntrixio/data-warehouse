/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
	The output Dimension table for loyalty cards

Parameters:
    ref_object      - stg_hermes__SCHEME_SCHEMEACCOUNT
    ref_object      - stg_hermes__SCHEME_SCHEME
    ref_object      - stg_hermes__SCHEME_CATEGORY
*/


With loyalty_card as (
    select * 
    from {{ref('stg_hermes__SCHEME_SCHEMEACCOUNT')}}
)

, loyalty_plan as (
    select * 
    from {{ref('stg_hermes__SCHEME_SCHEME')}}
)

, loyalty_plan_category as (
    select * 
    from {{ref('stg_hermes__SCHEME_CATEGORY')}}
)

, join_to_base as (
    select 
    -- BALANCES --is this a json field
       LOYALTY_CARD_ID
       ,LINK_DATE
       ,JOIN_DATE
       ,CARD_NUMBER
       ,UPDATED
       ,BARCODE
    --    ,VOUCHERS  --is this a json field
       ,CREATED
    --    ,MAIN_ANSWER --??? what does this relate to
       ,ORDERS
    --    ,TRANSACTIONS
       ,ORIGINATING_JOURNEY -- is there a linking table for this ?
    --    ,PLL_LINKS  --is this a json field
    --    ,FORMATTED_IMAGES --is this a json field
       ,IS_DELETED
       ,lc.LOYALTY_PLAN_ID
       ,lp.Loylaty_plan_COMPANY
       ,lp.LOYALTY_PLAN_SLUG
       ,lp.LOYALTY_PLAN_TIER
       ,lp.LOYALTY_PLAN_NAME_CARD
       ,lp.LOYALTY_PLAN_NAME
       ,lp.LOYALTY_PLAN_CATEGORY_ID
       ,lpc.LOYALTY_PLAN_CATEGORY
    from loyalty_card lc
    left join loyalty_plan lp 
    on lc.LOYALTY_PLAN_ID = lp.LOYALTY_PLAN_ID
    left join loyalty_plan_category lpc
    on lp.LOYALTY_PLAN_CATEGORY_ID = lpc.LOYALTY_PLAN_CATEGORY_ID


)


select * from join_to_base 