/*
Created by:         Christopher Mitchell
Created date:       2023-07-05
Last modified by:
Last modified date:

Description:
    Datasource to produce lloyds mi dashboard - vouchers_overview
Parameters:
    source_object       - voucher__daily_channel_brand_retailer
                        - voucher__times__voucher_level_channel_brand
*/
with
voucher_daily as (
    select
        *,
        'DAILY' as tab
    from {{ ref("voucher__counts__daily_channel_brand_retailer") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Bink Sweet Shop', 'Loyalteas')
),

voucher_times as (
    select
        *,
        'TIMES' as tab
    from {{ ref("voucher__times__voucher_level_channel_brand") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Bink Sweet Shop', 'Loyalteas')
),

union_all as (
    select
        tab,
        date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        v004__issued_vouchers__daily_channel_brand_retailer__count,
        v005__redeemed_vouchers__daily_channel_brand_retailer__count,
        v006__expired_vouchers__daily_channel_brand_retailer__count,
        v001__issued_vouchers__daily_channel_brand_retailer__cdsum_voucher,
        v002__redeemed_vouchers__daily_channel_brand_retailer__cdsum_voucher,
        v003__expired_vouchers__daily_channel_brand_retailer__cdsum_voucher,
        null as state,
        null as earn_type,
        null as voucher_code,
        null as redemption_tracked,
        null as date_redeemed,
        null as date_issued,
        null as expiry_date,
        null as v007__time_to_redemption__voucher_level__sum,
        null as v008__days_left_on_voucher__voucher_level__sum
    from voucher_daily
    union all
    select
        tab,
        null as date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        null as v004__issued_vouchers__daily_channel_brand_retailer__count,
        null as v005__redeemed_vouchers__daily_channel_brand_retailer__count,
        null as v006__expired_vouchers__daily_channel_brand_retailer__count,
        null
            as v001__issued_vouchers__daily_channel_brand_retailer__cdsum_voucher,
        null
            as v002__redeemed_vouchers__daily_channel_brand_retailer__cdsum_voucher
        ,
        null
            as v003__expired_vouchers__daily_channel_brand_retailer__cdsum_voucher,
        state,
        earn_type,
        voucher_code,
        redemption_tracked,
        date_redeemed,
        date_issued,
        expiry_date,
        v007__time_to_redemption__voucher_level__sum,
        v008__days_left_on_voucher__voucher_level__sum
    from voucher_times
)

select *
from union_all
