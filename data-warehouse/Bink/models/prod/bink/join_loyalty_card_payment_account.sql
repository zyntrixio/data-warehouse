/*
 Created by:         Sam Pibworth
 Created date:       2022-06-23
Last modified by:   Anand Bhakta
Last modified date: 2023-12-11
 Description:
 Joining table, containing laid pll links from the scheme table and payment account table

 Parameters:
 ref_object      	- stg_hermes__SCHEME_SCHEMEACCOUNT
 				 	- stg_hermes__PAYMENT_ACCOUNT
 */

 {{ config(
  enabled=false
) }}

with
scheme as (
    select
        loyalty_card_id,
        parse_json(pll_links) as pll
    from {{ ref("stg_hermes__SCHEME_SCHEMEACCOUNT") }}
    where pll_links != '[]'
),

scheme_plls as (
    select
        loyalty_card_id,
        lf.value:"active_link"::boolean as active_link,
        lf.value:"id"::int as pll_link_id
    from scheme, lateral flatten(input => pll) lf
),

payment_accounts as (
    select
        payment_account_id,
        parse_json(pll_links) as pll
    from {{ ref("stg_hermes__PAYMENT_ACCOUNT") }}
    where pll_links != '[]'
),

payment_account_plls as (
    select
        payment_account_id,
        lf.value:"active_link"::boolean as active_link,
        lf.value:"id"::int as pll_link_id
    from payment_accounts, lateral flatten(input => pll) lf
),

intersecting_plls as (
    select pll_link_id
    from scheme_plls
    intersect
    select pll_link_id
    from payment_account_plls
),

joined_links as (
    select
        concat(loyalty_card_id, '-', payment_account_id) as pll_link_pk,
        s.loyalty_card_id::varchar as loyalty_card_id,
        p.payment_account_id::varchar as payment_account_id,
        i.pll_link_id,
        case
            when s.active_link and p.active_link then true else false
        end as active_link
    from intersecting_plls i
    inner join scheme_plls s on s.pll_link_id = i.pll_link_id
    inner join payment_account_plls p on i.pll_link_id = p.pll_link_id
)

select *
from joined_links
