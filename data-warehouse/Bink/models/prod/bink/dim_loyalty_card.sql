/*
Created by:         Sam Pibworth
Created date:       2022-06-14
Last modified by:   Anand Bhakta
Last modified date: 2023-12-11

Description:
	Dim loyalty card with reduced columns

Parameters:
    ref_object      - dim_loyalty_card_secure
*/

{{ config(
  enabled=false
) }}

with
loyalty_card as (select * from {{ ref("dim_loyalty_card_secure") }}),

loyalty_card_select as (
    select
        loyalty_card_id,
        -- add_auth_status,
        -- add_auth_date_time,
        -- join_status,
        -- join_date_time,
        -- register_status,
        -- register_date_time,
        -- card_number,
        updated,
        -- barcode,
        link_date,
        created,
        orders,
        originating_journey,
        is_deleted,
        loyalty_plan_id,
        loyalty_plan_company,
        loyalty_plan_slug,
        loyalty_plan_tier,
        loyalty_plan_name_card,
        loyalty_plan_name,
        loyalty_plan_category_id
    from loyalty_card
)

select *
from loyalty_card_select
