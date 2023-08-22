with
    source as (select * from {{ ref("stg_hermes__SCHEME_SCHEMEACCOUNT") }}),
    renaming as (
        select
            loyalty_card_id as loyalty_card_id,
            loyalty_plan_id as loyalty_plan_id,
            created as created,
            v.value:code::string as code,
            v.value:barcode_type::string as barcode_type,
            upper(v.value:body_text::string) as body_text,
            upper(v.value:burn.currency::string) as burn_currency,
            upper(v.value:burn.prefix::string) as burn_prefix,
            upper(v.value:burn.suffix::string) as burn_suffix,
            upper(v.value:burn.type::string) as burn_type,
            v.value:burn.value::float as burn_value,
            upper(v.value:earn.currency::string) as earn_currency,
            upper(v.value:earn.prefix::string) as earn_prefix,
            upper(v.value:earn.suffix::string) as earn_suffix,
            v.value:earn.target_value::float as earn_target_value,
            upper(v.value:earn.type::string) as earn_type,
            v.value:earn.value::float as earn_value,
            upper(v.value:headline::string) as headline,
            upper(v.value:state::string) as state,
            upper(v.value:subtext::string) as subtext,
            upper(v.value:terms_and_conditions_url::string) as terms_and_conditions_url,
            v.value:date_redeemed::timestamp_ntz as date_redeemed,
            v.value:date_issued::timestamp_ntz as date_issued,
            v.value:expiry_date::timestamp_ntz as expiry_date
        from source, lateral flatten(parse_json(vouchers)) as v
    )

select *
from renaming
