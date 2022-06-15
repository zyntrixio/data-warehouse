/*
Created by:         Sam Pibworth
Created date:       2022-06-14
Last modified by:   
Last modified date: 

Description:
	Dim loyalty card with reduced columns

Parameters:
    ref_object      - dim_loyalty_card_secure
*/

WITH
loyalty_card AS (
    SELECT * 
    FROM {{ref('dim_loyalty_card_secure')}}
)

, loyalty_card_select AS (
    SELECT
        LOYALTY_CARD_ID
        ,ADD_AUTH_STATUS
        ,ADD_AUTH_DATE_TIME
        ,JOIN_STATUS
        ,JOIN_DATE_TIME
        ,REGISTER_STATUS
        ,REGISTER_DATE_TIME
        // ,CARD_NUMBER
        ,UPDATED
        ,STATUS_ID
        ,STATUS
        ,STATUS_TYPE
        ,STATUS_ROLLUP
        // ,BARCODE
        ,LINK_DATE
        ,CREATED
        ,ORDERS
        ,ORIGINATING_JOURNEY
        ,IS_DELETED
        ,LOYALTY_PLAN_ID
        ,LOYALTY_PLAN_COMPANY
        ,LOYALTY_PLAN_SLUG
        ,LOYALTY_PLAN_TIER
        ,LOYALTY_PLAN_NAME_CARD
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_CATEGORY_ID
        ,LOYALTY_PLAN_CATEGORY
    FROM
        loyalty_card
)


SELECT *
FROM loyalty_card_select 