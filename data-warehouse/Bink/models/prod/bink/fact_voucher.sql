/*
Created by:         Sam Pibworth
Created date:       2022-05-04
Last modified by:   Anand Bhakta
Last modified date: 2023-12-11

Description:
    One row per voucher with links loyalty_card_id and loyalty_plan
If there are duplicated it takes the latest loyalty plan by created date

Parameters:
    ref_object      - transformed_voucher-keys
                    - dim_lc

*/
with
vouchers as (select * from {{ ref("transformed_voucher_keys") }}),

loyalty_card as (select * from {{ ref("stg_hermes__SCHEME_SCHEME") }}),

add_company as (

    select
        v.created,
        v.loyalty_card_id,
        lc.loyalty_plan_company,
        lc.loyalty_plan_name,
        v.state,
        v.earn_type,
        v.voucher_code,
        v.date_redeemed,
        v.date_issued,
        v.expiry_date,
        case
            when lc.loyalty_plan_company = 'ASOS'
                then 'FALSE'
            when state = 'CANCELLED'
                then 'FALSE'
            else 'TRUE'
        end as redemption_tracked
    from vouchers v
    left join loyalty_card lc on v.loyalty_plan_id = lc.loyalty_plan_id

),

timings as (
    select
        created,
        loyalty_card_id,
        loyalty_plan_company,
        loyalty_plan_name,
        state,
        earn_type,
        voucher_code,
        redemption_tracked,
        date_redeemed,
        date_issued,
        expiry_date,
        case
            when redemption_tracked = 'TRUE' and state in ('ISSUED', 'REDEEMED')
                then
                    datediff(
                        day,
                        date_issued,
                        coalesce(date_redeemed, current_date())
                    )
            else null
        end as time_to_redemption,
        case
            when
                state = 'ISSUED'
                and redemption_tracked = 'TRUE'
                and expiry_date >= current_date()
                then datediff(day, current_date(), expiry_date)
            else null
        end as days_left_on_vouchers,
        datediff(day, date_issued, expiry_date) as days_valid_for
    from add_company
)

select *
from
    timings
    -- where current_channel = 'com.barclays.bmb'
