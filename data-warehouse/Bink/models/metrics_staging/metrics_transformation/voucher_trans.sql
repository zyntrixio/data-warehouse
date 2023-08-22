/*
Created by:         Christopher Mitchell
Created date:       2023-06-30
Last modified by:
Last modified date:

Description:
    Voucher table, which relates to the transform date into do date and from date for metrics layer

Parameters:
    ref_object      - src__fact_voucher
*/
with
fact_voucher as (select * from {{ ref("stg_metrics__fact_voucher") }}),

lc_trans as (select * from {{ ref("lc_trans") }} where event_type = 'SUCCESS'),

issued as (
    select
        v.loyalty_card_id,
        v.state,
        v.earn_type,
        v.voucher_code,
        v.redemption_tracked,
        v.date_redeemed,
        v.date_issued,
        v.expiry_date,
        v.time_to_redemption,
        v.days_valid_for,
        v.days_left_on_vouchers,
        l.user_id,
        l.channel,
        l.brand,
        l.loyalty_plan_company,
        l.loyalty_plan_name
    from fact_voucher v
    inner join
        lc_trans l
        on
            v.date_issued between l.from_date and l.to_date
            and v.loyalty_card_id = l.loyalty_card_id
),

redeem as (
    select
        v.loyalty_card_id,
        v.state,
        v.earn_type,
        v.voucher_code,
        v.redemption_tracked,
        v.date_redeemed,
        v.date_issued,
        v.expiry_date,
        v.time_to_redemption,
        v.days_valid_for,
        v.days_left_on_vouchers,
        l.user_id,
        l.channel,
        l.brand,
        l.loyalty_plan_company,
        l.loyalty_plan_name
    from fact_voucher v
    inner join
        lc_trans l
        on
            v.date_redeemed between l.from_date and l.to_date
            and v.loyalty_card_id = l.loyalty_card_id

),

final as (

    select
        coalesce(i.loyalty_card_id, r.loyalty_card_id) as loyalty_card_id,
        coalesce(i.user_id, r.user_id) as user_id,
        coalesce(i.channel, r.channel) as channel,
        coalesce(i.brand, r.brand) as brand,
        coalesce(
            i.loyalty_plan_company, r.loyalty_plan_company
        ) as loyalty_plan_company,
        coalesce(i.loyalty_plan_name, r.loyalty_plan_name) as loyalty_plan_name,
        coalesce(i.state, r.state) as state,
        coalesce(i.earn_type, r.earn_type) as earn_type,
        coalesce(i.voucher_code, i.voucher_code) as voucher_code,
        coalesce(i.redemption_tracked, r.redemption_tracked)
            as redemption_tracked,
        coalesce(r.date_redeemed, null) as date_redeemed,
        coalesce(i.date_issued, null) as date_issued,
        coalesce(i.expiry_date, r.expiry_date) as expiry_date,
        case
            when i.date_issued is not null and r.date_redeemed is not null
                then coalesce(i.time_to_redemption, r.time_to_redemption)
            else null
        end as time_to_redemption,
        case
            when i.date_issued is not null and r.date_redeemed is not null
                then coalesce(i.days_valid_for, r.days_valid_for)
            else null
        end as days_valid_for,
        coalesce(
            i.days_left_on_vouchers, r.days_left_on_vouchers
        ) as days_left_on_vouchers

    from issued i
    full outer join
        redeem r
        on
            r.loyalty_card_id = i.loyalty_card_id
            and r.voucher_code = i.voucher_code
            and r.user_id = i.user_id
            and r.channel = r.channel
    order by i.voucher_code

)

select *
from final
