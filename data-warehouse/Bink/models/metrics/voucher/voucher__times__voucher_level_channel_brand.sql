/*
Created by:         Christopher Mitchell
Created date:       2023-07-04
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    Voucher Date related metrics on a voucher level by channel and brand
Parameters:
    source_object       - voucher_trans
*/
with
voucher_trans as (select * from {{ ref("voucher_trans") }}),

metrics as (
    select distinct
        channel,
        brand,
        loyalty_plan_company,
        loyalty_plan_name,
        state,
        earn_type,
        voucher_code,
        redemption_tracked,
        date_redeemed,
        date_issued,
        expiry_date,
        time_to_redemption as v007__time_to_redemption__voucher_level__sum,
        days_left_on_vouchers as v008__days_left_on_voucher__voucher_level__sum
    from voucher_trans

)

select *
from metrics
