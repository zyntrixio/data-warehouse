with source as (
    select * 
    from {{ref('stg_hermes__SCHEME_SCHEMEACCOUNT')}}
)


, renaming as (
SELECT  LOYALTY_CARD_ID                                 AS LOYALTY_CARD_ID
       ,LOYALTY_PLAN_ID                                 AS LOYALTY_PLAN_ID
       ,CREATED                                         AS CREATED
       ,v.value:code::string                            AS code
       ,v.value:barcode_type::string                    AS barcode_type
       ,upper(v.value:body_text::string )               AS body_text
       ,upper(v.value:burn.currency::string)            AS burn_currency
       ,upper(v.value:burn.prefix::string)              AS burn_prefix
       ,upper(v.value:burn.suffix::string)              AS burn_suffix
       ,upper(v.value:burn.type::string)                AS burn_type
       ,v.value:burn.value::float                       AS burn_value
       ,upper(v.value:earn.currency::string)            AS earn_currency
       ,upper(v.value:earn.prefix::string)              AS earn_prefix
       ,upper(v.value:earn.suffix::string)              AS earn_suffix
       ,v.value:earn.target_value::float                AS earn_target_value
       ,upper(v.value:earn.type::string )               AS earn_type
       ,v.value:earn.value::float                       AS earn_value
       ,upper(v.value:headline::string)                 AS headline
       ,upper(v.value:state::string)                    AS state
       ,upper(v.value:subtext::string)                  AS subtext
       ,upper(v.value:terms_and_conditions_url::string) AS terms_and_conditions_url
       ,v.value:date_redeemed::timestamp_ntz            AS date_redeemed
       ,v.value:date_issued::timestamp_ntz              AS date_issued
       ,v.value:expiry_date::timestamp_ntz              AS expiry_date
FROM source,
 lateral flatten(parse_json(vouchers)) AS v
)


select * from renaming