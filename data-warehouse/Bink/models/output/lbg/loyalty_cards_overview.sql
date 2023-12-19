/*
Created by:         Anand Bhakta
Created date:       2023-06-07
Last modified by: Anand Bhakta
Last modified date: 2023-12-19
Description:
    Datasource to produce lloyds mi dashboard - loyalty_cards_overview
Parameters:
    source_object       - lc__links_joins__daily_channel_brand_retailer
                        - user__loyalty_card__daily_channel_brand
*/
with
lc_metrics as (
    select
        *,
        'LC' as tab
    from {{ ref("lc__links_joins__daily_channel_brand_retailer") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Loyalteas', 'Bink Sweet Shop')
),

lc_user_metrics as (
    select
        *,
        'LC_USER' as tab
    from {{ ref("user__loyalty_card__daily_channel_brand") }}
    where channel = 'LLOYDS'
),

combine as (
    select
        tab,
        date,
        channel,
        brand,
        loyalty_plan_company,
        lc001__successful_loyalty_cards__daily_channel_brand_retailer__pit,
        lc009__successful_loyalty_card_links__daily_channel_brand_retailer__count,
        lc010__requests_loyalty_card_links__daily_channel_brand_retailer__count,
        lc011__failed_loyalty_card_links__daily_channel_brand_retailer__count,
        lc012__deleted_loyalty_card_links__daily_channel_brand_retailer__count,
        lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count,
        lc014__requests_loyalty_card_joins__daily_channel_brand_retailer__count,
        lc015__failed_loyalty_card_joins__daily_channel_brand_retailer__count,
        lc016__deleted_loyalty_card_joins__daily_channel_brand_retailer__count,
        lc017__successful_loyalty_card_links__daily_channel_brand_retailer__dcount_user
        ,
        lc018__requests_loyalty_card_links__daily_channel_brand_retailer__dcount_user
        ,
        lc019__failed_loyalty_card_links__daily_channel_brand_retailer__dcount_user,
        lc020__deleted_loyalty_card_links__daily_channel_brand_retailer__dcount_user
        ,
        lc021__successful_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
        ,
        lc022__requests_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
        ,
        lc023__failed_loyalty_card_joins__daily_channel_brand_retailer__dcount_user,
        lc024__deleted_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
        ,
        lc025__successful_loyalty_card_links__daily_channel_brand_retailer__pit,
        lc026__requests_loyalty_card_links__daily_channel_brand_retailer__pit,
        lc027__failed_loyalty_card_links__daily_channel_brand_retailer__pit,
        lc028__deleted_loyalty_card_links__daily_channel_brand_retailer__pit,
        lc029__successful_loyalty_card_joins__daily_channel_brand_retailer__pit,
        lc030__requests_loyalty_card_joins__daily_channel_brand_retailer__pit,
        lc031__failed_loyalty_card_joins__daily_channel_brand_retailer__pit,
        lc032__deleted_loyalty_card_joins__daily_channel_brand_retailer__pit,
        null as u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit
    from lc_metrics

    union all

    select
        tab,
        date,
        channel,
        brand,
        null as loyalty_plan_company,
        null
            as lc001__successful_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as lc009__successful_loyalty_card_links__daily_channel_brand_retailer__count
        ,
        null
            as lc010__requests_loyalty_card_links__daily_channel_brand_retailer__count,
        null
            as lc011__failed_loyalty_card_links__daily_channel_brand_retailer__count,
        null
            as lc012__deleted_loyalty_card_links__daily_channel_brand_retailer__count,
        null
            as lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count
        ,
        null
            as lc014__requests_loyalty_card_joins__daily_channel_brand_retailer__count,
        null
            as lc015__failed_loyalty_card_joins__daily_channel_brand_retailer__count,
        null
            as lc016__deleted_loyalty_card_joins__daily_channel_brand_retailer__count,
        null
            as lc017__successful_loyalty_card_links__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc018__requests_loyalty_card_links__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc019__failed_loyalty_card_links__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc020__deleted_loyalty_card_links__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc021__successful_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc022__requests_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc023__failed_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc024__deleted_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
        ,
        null
            as lc025__successful_loyalty_card_links__daily_channel_brand_retailer__pit,
        null
            as lc026__requests_loyalty_card_links__daily_channel_brand_retailer__pit,
        null
            as lc027__failed_loyalty_card_links__daily_channel_brand_retailer__pit,
        null
            as lc028__deleted_loyalty_card_links__daily_channel_brand_retailer__pit
        ,
        null
            as lc029__successful_loyalty_card_joins__daily_channel_brand_retailer__pit,
        null
            as lc030__requests_loyalty_card_joins__daily_channel_brand_retailer__pit,
        null
            as lc031__failed_loyalty_card_joins__daily_channel_brand_retailer__pit,
        null
            as lc032__deleted_loyalty_card_joins__daily_channel_brand_retailer__pit
        ,
        u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit
    from lc_user_metrics
)

select *
from combine
