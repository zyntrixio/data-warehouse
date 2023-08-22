/*
Created by:         Sam Pibworth
Created date:       2022-07-08
Last modified by:   Christopher Mitchell
Last modified date: 2023-05-06

Description:
	Dim table for merchants

Parameters:
    ref_object      - stg_harmonia__merchant_identifier
*/
with
    merchant_identifier as (
        select * from {{ ref("stg_harmonia__merchant_identifier") }}
    ),
    payment_provider as (select * from {{ ref("stg_harmonia__payment_provider") }}),
    loyalty_scheme as (select * from {{ ref("stg_harmonia__loyalty_scheme") }}),
    consolidate_multiple_locations as (
        select
            m.merchant_id,
            last_value(m.location) over (
                partition by m.merchant_id order by m.id
            ) as location,
            last_value(m.postcode) over (
                partition by m.merchant_id order by m.id
            ) as postcode,
            last_value(m.location_id) over (
                partition by m.merchant_id order by m.id
            ) as location_id,
            m.loyalty_scheme_id,
            m.payment_provider_id
        from merchant_identifier m
    ),
    merchant_select as (
        select
            m.merchant_id,
            m.location,
            m.postcode,
            m.location_id,
            m.loyalty_scheme_id,
            l.slug as loyalty_scheme_slug,
            case
                when array_contains('visa'::variant, array_agg(p.slug))
                then true
                else false
            end as payment_provider_visa,
            case
                when array_contains('mastercard'::variant, array_agg(p.slug))
                then true
                else false
            end as payment_provider_mastercard,
            case
                when array_contains('amex'::variant, array_agg(p.slug))
                then true
                else false
            end as payment_provider_amex
        from consolidate_multiple_locations m
        left join payment_provider p on m.payment_provider_id = p.id
        left join loyalty_scheme l on m.loyalty_scheme_id = l.id
        group by
            m.merchant_id,
            m.location,
            m.postcode,
            m.location_id,
            m.loyalty_scheme_id,
            l.slug
    )

select *
from merchant_select
