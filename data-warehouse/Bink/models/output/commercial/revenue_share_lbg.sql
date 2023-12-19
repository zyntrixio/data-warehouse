/*
Created by:          Christopher Mitchell
Created date:        2023-07-04
Last modified by:    Christopher Mitchell
Last modified date:  2023-08-23

Description:
    Datasource to produce lloyds mi dashboard - LBG Revenue Share
Parameters:
    source_object       - lc__links_joins__monthly_channel_brand_retailer
                        - user__transactions__monthly_channel_brand_retailer
*/
with
joins as (
    select
        date,
        channel,
        brand,
        loyalty_plan_name,
        loyalty_plan_company,
        lc328__successful_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user,
        'JOINS' as tab
    from {{ ref("lc__links_joins__monthly_channel_brand_retailer") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Loyalteas', 'Bink Sweet Shop')
),

active as (
    select
        date,
        channel,
        brand,
        loyalty_plan_company,
        u109__active_users__monthly_channel_brand_retailer__dcount_uid,
        'ACTIVE' as tab
    from {{ ref("user__transactions__monthly_channel_brand_retailer") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Loyalteas', 'Bink Sweet Shop')
),

combine as (
    select
        date,
        tab,
        channel,
        brand,
        loyalty_plan_company,
        lc328__successful_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
        ,
        null as u109__active_users__monthly_channel_brand_retailer__dcount_uid
    from joins
    union all
    select
        date,
        tab,
        channel,
        brand,
        loyalty_plan_company,
        null
            as lc328__successful_loyalty_card_joins__monthly_channel_brand_retailer__dcount_user
        ,
        u109__active_users__monthly_channel_brand_retailer__dcount_uid
    from active
)

select *
from combine
