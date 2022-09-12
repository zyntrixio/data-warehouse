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

{{ config(alias='dim_loyalty_card') }}

WITH
loyalty_card AS (
    SELECT * 
    FROM {{ref('stg_hermes__SCHEME_SCHEMEACCOUNT')}}
)

, loyalty_plan AS (
    SELECT * 
    FROM {{ref('stg_hermes__SCHEME_SCHEME')}}
)

, loyalty_plan_category AS (
    SELECT * 
    FROM {{ref('stg_hermes__SCHEME_CATEGORY')}}
)

, account_status AS (
    SELECT *
    FROM {{ref('stg_lookup__SCHEME_ACCOUNT_STATUS')}}
)

,lc_add_auth AS (
    SELECT *
    FROM {{ref('fact_loyalty_card_add_auth_secure')}}
    WHERE IS_MOST_RECENT = TRUE
)

, lc_join AS (
    SELECT *
    FROM {{ref('fact_loyalty_card_join_secure')}}
    WHERE IS_MOST_RECENT = TRUE
)

, lc_register AS (
    SELECT *
    FROM {{ref('fact_loyalty_card_register_secure')}}
    WHERE IS_MOST_RECENT = TRUE
)


, join_to_base AS (
    SELECT 
    -- BALANCES --is this a json field
        lc.LOYALTY_CARD_ID
        ,lcaa.EVENT_TYPE AS ADD_AUTH_STATUS
        ,lcaa.EVENT_DATE_TIME AS ADD_AUTH_DATE_TIME
        ,lcj.EVENT_TYPE AS JOIN_STATUS
        ,COALESCE(lcj.EVENT_DATE_TIME, lc.JOIN_DATE) AS JOIN_DATE_TIME
        ,lcr.EVENT_TYPE AS REGISTER_STATUS
        ,lcr.EVENT_DATE_TIME AS REGISTER_DATE_TIME
        ,CARD_NUMBER
        ,UPDATED
        ,lc.STATUS AS STATUS_ID
        ,a.STATUS
        ,a.STATUS_TYPE
        ,a.STATUS_ROLLUP
        ,BARCODE
        ,LINK_DATE
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
        ,lp.LOYALTY_PLAN_COMPANY
        ,lp.LOYALTY_PLAN_SLUG
        ,lp.LOYALTY_PLAN_TIER
        ,lp.LOYALTY_PLAN_NAME_CARD
        ,lp.LOYALTY_PLAN_NAME
        ,lp.LOYALTY_PLAN_CATEGORY_ID
        ,lpc.LOYALTY_PLAN_CATEGORY
    FROM loyalty_card lc
    LEFT JOIN loyalty_plan lp 
        ON lc.LOYALTY_PLAN_ID = lp.LOYALTY_PLAN_ID
    LEFT JOIN loyalty_plan_category lpc
        ON lp.LOYALTY_PLAN_CATEGORY_ID = lpc.LOYALTY_PLAN_CATEGORY_ID
    LEFT JOIN account_status a
        ON lc.STATUS = a.CODE
    LEFT JOIN lc_add_auth lcaa
        ON lc.LOYALTY_CARD_ID = lcaa.LOYALTY_CARD_ID
    LEFT JOIN lc_join lcj
        ON lc.LOYALTY_CARD_ID = lcj.LOYALTY_CARD_ID
    LEFT JOIN lc_register lcr
        ON lc.LOYALTY_CARD_ID = lcr.LOYALTY_CARD_ID


)


SELECT *
FROM join_to_base 