/*
Created by:         Christopher Mitchell
Created date:       2023-10-03
Last modified by:   
Last modified date: 

Description:
    Count of Voucher States daily by channel brand and retailer
Parameters:
    source_object       - voucher_trans
                        - dim_date
*/

with
voucher_trans as (select * from {{ ref("voucher_trans") }}),

dim_date as (
    select start_of_month, end_of_month
    from {{ ref("dim_date") }}
    where
        date >= (select min(date_issued) from voucher_trans)
        and date <= current_date()
),

voucher_staging as (
    select
        v.date_issued as date,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        v.voucher_code,
        'ISSUED' as state
    from voucher_trans v
    union
    select
        v.date_redeemed as date,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        v.voucher_code,
        v.state
    from voucher_trans v
    where state = 'REDEEMED'
    union
    select
        v.expiry_date as date,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        v.voucher_code,
        v.state
    from voucher_trans v
    where state = 'EXPIRED'
),

voucher_metrics as (
    select
        d.start_of_month as date,
        v.loyalty_plan_company,
        v.loyalty_plan_name,
        coalesce(
            count(distinct case when state = 'ISSUED' then voucher_code end), 0
        ) as monthly_issued_vouchers,
        coalesce(
            count(distinct case when state = 'REDEEMED' then voucher_code end),
            0
        ) as monthly_redeemed_vouchers,
        coalesce(
            count(distinct case when state = 'EXPIRED' then voucher_code end), 0
        ) as monthly_expired_vouchers
    from dim_date d
    left join voucher_staging v on d.start_of_month = date_trunc('month', v.date)
    group by
        d.start_of_month, v.loyalty_plan_company, v.loyalty_plan_name
),

rename as (
    select
        date,
        loyalty_plan_company,
        loyalty_plan_name,
        monthly_issued_vouchers
            as v012__issued_vouchers__monthly_retailer__count,
        monthly_redeemed_vouchers
            as v013__redeemed_vouchers__monthly_retailer__count,
        monthly_expired_vouchers
            as v014__expired_vouchers__monthly_retailer__count,
        sum(monthly_issued_vouchers) over (
            partition by loyalty_plan_company order by date asc
        ) as v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        sum(monthly_redeemed_vouchers)
            over (
                partition by loyalty_plan_company order by date asc
            )
            as v010__redeemed_vouchers__monthly_retailer__cdsum_voucher,
        sum(monthly_expired_vouchers) over (
            partition by loyalty_plan_company order by date asc
        ) as v011__expired_vouchers__monthly_retailer__cdsum_voucher
    from voucher_metrics
)

select *
from rename
