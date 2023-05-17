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
{{ config(
    alias = 'dim_loyalty_card'
) }}

WITH loyalty_card AS (

    SELECT
        *
    FROM
        {{ ref('stg_hermes__SCHEME_SCHEMEACCOUNT') }}
),
loyalty_plan AS (
    SELECT
        *
    FROM
        {{ ref('stg_hermes__SCHEME_SCHEME') }}
),
loyalty_plan_category AS (
    SELECT
        *
    FROM
        {{ ref('stg_hermes__SCHEME_CATEGORY') }}
),

join_to_base AS (
    SELECT
        -- BALANCES --is this a json field
        lc.loyalty_card_id,
         --    ,lcaa.EVENT_TYPE AS ADD_AUTH_STATUS
        --    ,lcaa.EVENT_DATE_TIME AS ADD_AUTH_DATE_TIME
        --    ,lcj.EVENT_TYPE AS JOIN_STATUS
        --    ,COALESCE(lcj.EVENT_DATE_TIME, lc.JOIN_DATE) AS JOIN_DATE_TIME
        --    ,lcr.EVENT_TYPE AS REGISTER_STATUS
        --    ,lcr.EVENT_DATE_TIME AS REGISTER_DATE_TIME,
        card_number,
        updated,
        barcode,
        link_date,
         --    ,VOUCHERS  --is this a json field,
        created,
        orders,
         --    TRANSACTIONS,
        originating_journey, -- is there a linking table for this ?
        --    ,PLL_LINKS  --is this a json field
        --    ,FORMATTED_IMAGES --is this a json field,
        is_deleted,
        lc.loyalty_plan_id,
        lp.loyalty_plan_company,
        lp.loyalty_plan_slug,
        lp.loyalty_plan_tier,
        lp.loyalty_plan_name_card,
        lp.loyalty_plan_name,
        lp.loyalty_plan_category_id,
        lpc.loyalty_plan_category
    FROM
        loyalty_card lc
        LEFT JOIN loyalty_plan lp
        ON lc.loyalty_plan_id = lp.loyalty_plan_id
        LEFT JOIN loyalty_plan_category lpc
        ON lp.loyalty_plan_category_id = lpc.loyalty_plan_category_id
)
SELECT
    *
FROM
    join_to_base
