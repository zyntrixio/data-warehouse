/*
Created by:         Christopher Mitchell
Created date:       2023-07-04
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    Count of Voucher States daily by channel brand and retailer
Parameters:
    source_object       - voucher_trans
                        - dim_date
*/
with
voucher_trans as (select * from {{ ref("voucher_trans") }}),

dim_date as (
    select *
    from {{ ref("dim_date") }}
    where
        date >= (select min(date_issued) from voucher_trans)
        and date <= current_date()
),

voucher_staging as (
    select
        v.date_issued as date,
        v.channel,
        v.brand,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        v.voucher_code,
        'ISSUED' as state
    from voucher_trans v
    union
    select
        v.date_redeemed as date,
        v.channel,
        v.brand,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        v.voucher_code,
        v.state
    from voucher_trans v
    where state = 'REDEEMED'
    union
    select
        v.expiry_date as date,
        v.channel,
        v.brand,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        v.voucher_code,
        v.state
    from voucher_trans v
    where state = 'EXPIRED'
),

voucher_metrics as (
    select
        d.date,
        v.channel,
        v.brand,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        coalesce(
            count(distinct case when state = 'ISSUED' then voucher_code end), 0
        ) as daily_issued_vouchers,
        coalesce(
            count(distinct case when state = 'REDEEMED' then voucher_code end),
            0
        ) as daily_redeemed_vouchers,
        coalesce(
            count(distinct case when state = 'EXPIRED' then voucher_code end), 0
        ) as daily_expired_vouchers
    from dim_date d
    left join voucher_staging v on d.date = date(v.date)
    group by
        d.date, v.channel, v.brand, v.loyalty_plan_company, v.loyalty_plan_name
),

rename as (
    select
        date,
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        daily_issued_vouchers
            -- these should be d count metrics
            as v004__issued_vouchers__daily_channel_brand_retailer__count,
        daily_redeemed_vouchers
            -- these should be d count metrics
            as v005__redeemed_vouchers__daily_channel_brand_retailer__count,
        daily_expired_vouchers
            -- these should be d count metrics
            as v006__expired_vouchers__daily_channel_brand_retailer__count,
        sum(daily_issued_vouchers) over (
            partition by loyalty_plan_company, brand order by date asc
        ) as v001__issued_vouchers__daily_channel_brand_retailer__cdsum_voucher,
        sum(daily_redeemed_vouchers)
            over (
                partition by loyalty_plan_company, brand order by date asc
            )
            as v002__redeemed_vouchers__daily_channel_brand_retailer__cdsum_voucher,
        sum(daily_expired_vouchers) over (
            partition by loyalty_plan_company, brand order by date asc
        ) as v003__expired_vouchers__daily_channel_brand_retailer__cdsum_voucher
    from voucher_metrics
    where channel is not null
)

select *
from rename
