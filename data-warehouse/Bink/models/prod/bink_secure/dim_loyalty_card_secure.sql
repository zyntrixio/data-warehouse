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
{{ config(alias="dim_loyalty_card",
            enabled =false) }}

with
loyalty_card as (select * from {{ ref("stg_hermes__SCHEME_SCHEMEACCOUNT") }}),

loyalty_plan as (select * from {{ ref("stg_hermes__SCHEME_SCHEME") }}),

join_to_base as (
    select
        -- BALANCES -- is this a json field
        lc.loyalty_card_id,
        -- ,lcaa.EVENT_TYPE AS ADD_AUTH_STATUS
        -- ,lcaa.EVENT_DATE_TIME AS ADD_AUTH_DATE_TIME
        -- ,lcj.EVENT_TYPE AS JOIN_STATUS
        -- ,COALESCE(lcj.EVENT_DATE_TIME, lc.JOIN_DATE) AS JOIN_DATE_TIME
        -- ,lcr.EVENT_TYPE AS REGISTER_STATUS
        -- ,lcr.EVENT_DATE_TIME AS REGISTER_DATE_TIME,
        card_number,
        updated,
        barcode,
        link_date,
        -- ,VOUCHERS  -- is this a json field,
        created,
        orders,
        -- TRANSACTIONS,
        originating_journey,  -- is there a linking table for this ?
        -- ,PLL_LINKS  -- is this a json field
        -- ,FORMATTED_IMAGES -- is this a json field,
        is_deleted,
        lc.loyalty_plan_id,
        lp.loyalty_plan_company,
        lp.loyalty_plan_slug,
        lp.loyalty_plan_tier,
        lp.loyalty_plan_name_card,
        lp.loyalty_plan_name,
        lp.loyalty_plan_category_id
    from loyalty_card lc
    left join loyalty_plan lp on lc.loyalty_plan_id = lp.loyalty_plan_id
)

select *
from join_to_base
