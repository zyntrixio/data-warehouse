with
vouchers as (select * from {{ ref("stg_hermes__VOUCHERS") }}),

de_dupe as (
    select
        loyalty_card_id,
        loyalty_plan_id,
        created,
        code as voucher_code,
        barcode_type,
        body_text,
        burn_currency,
        burn_prefix,
        burn_suffix,
        burn_type,
        burn_value,
        earn_currency,
        earn_prefix,
        earn_suffix,
        earn_target_value,
        earn_type,
        earn_value,
        headline,
        state,
        subtext,
        terms_and_conditions_url,
        date_redeemed,
        date_issued,
        expiry_date,
        row_number() over (
            partition by loyalty_card_id, code order by created desc
        ) as voucher_rank,
        row_number() over (
            partition by code order by created desc
        ) as voucher_rank_rev
    from vouchers
    where code not like 'Due:%'
)

select distinct
    d1.loyalty_card_id,
    d1.loyalty_plan_id,
    d1.created,
    d1.voucher_code,
    d1.barcode_type,
    d1.body_text,
    d1.burn_currency,
    d1.burn_prefix,
    d1.burn_suffix,
    d1.burn_type,
    d1.burn_value,
    d1.earn_currency,
    d1.earn_prefix,
    d1.earn_suffix,
    d1.earn_target_value,
    d1.earn_type,
    d1.earn_value,
    d1.headline,
    d1.state,
    d1.subtext,
    d1.terms_and_conditions_url,
    d1.date_redeemed,
    d1.date_issued,
    d1.expiry_date
from de_dupe d1
where d1.voucher_rank = 1
