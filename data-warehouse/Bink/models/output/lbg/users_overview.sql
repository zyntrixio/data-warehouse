/*
Created by:         Anand Bhakta
Created date:       2023-05-23
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-23

Description:
    Datasource to produce lloyds mi dashboard - users_overview
Parameters:
    source_object       - lc__links_joins__daily_retailer_channel
                        - user__loyalty_card__daily_channel_brand
                        - user__transactions__daily_user_level
                        - trans__trans__daily_user_level
*/
with
lc_metrics_retailer as (
    select
        *,
        'LC_RETAILER_CHANNEL' as tab
    from {{ ref("lc__links_joins__daily_retailer_channel") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Bink Sweet Shop', 'Loyalteas')
),

lc_metrics as (
    select
        *,
        'LC_CHANNEL' as tab
    from {{ ref("user__loyalty_card__daily_channel_brand") }}
    where channel = 'LLOYDS'
),

active_usr as (
    select
        *,
        'ACTIVE_USER' as tab
    from {{ ref("user__transactions__daily_user_level") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Bink Sweet Shop', 'Loyalteas')
),

txn as (
    select
        *,
        'TRANS' as tab
    from {{ ref("trans__trans__daily_user_level") }}
    where
        channel = 'LLOYDS'
        and loyalty_plan_company not in ('Bink Sweet Shop', 'Loyalteas')
),

users_metrics as (
    select
        *,
        'USERS' as tab
    from {{ ref("user__registrations__daily_channel_brand") }}
    where channel = 'LLOYDS'
),

metric_select as (
    select
        tab,
        date,
        channel,
        brand,
        loyalty_plan_company,
        lc001__successful_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit,
        lc004__deleted_loyalty_cards__daily_channel_brand_retailer__pit,
        lc005__successful_loyalty_cards__daily_channel_brand_retailer__count,
        lc008__deleted_loyalty_cards__daily_channel_brand_retailer__count,
        null as u005__registered_users__daily_channel_brand__count,
        null as u006__deregistered_users__daily_channel_brand__count,
        null as u001__registered_users__daily_channel_brand__pit,
        null as u002__deregistered_users__daily_channel_brand__pit,
        null as t001__spend__user_level_daily__sum,
        null as u007__active_users__user_level_daily__uid,
        null as t003__transactions__user_level_daily__dcount_txn
    from lc_metrics_retailer

    union all

    select
        tab,
        date,
        channel,
        brand,
        null as loyalty_plan_company,
        null
            as lc001__successful_loyalty_cards__daily_channel_brand_retailer__pit,
        u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit,
        null as lc004__deleted_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as lc005__successful_loyalty_cards__daily_channel_brand_retailer__count
        ,
        null
            as lc008__deleted_loyalty_cards__daily_channel_brand_retailer__count,
        null as u005__registered_users__daily_channel_brand__count,
        null as u006__deregistered_users__daily_channel_brand__count,
        null as u001__registered_users__daily_channel_brand__pit,
        null as u002__deregistered_users__daily_channel_brand__pit,
        null as t001__spend__user_level_daily__sum,
        null as u007__active_users__user_level_daily__uid,
        null as t003__transactions__user_level_daily__dcount_txn
    from lc_metrics

    union all

    select
        tab,
        date,
        channel,
        brand,
        loyalty_plan_company,
        null
            as lc001__successful_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit,
        null as lc004__deleted_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as lc005__successful_loyalty_cards__daily_channel_brand_retailer__count
        ,
        null
            as lc008__deleted_loyalty_cards__daily_channel_brand_retailer__count,
        null as u005__registered_users__daily_channel_brand__count,
        null as u006__deregistered_users__daily_channel_brand__count,
        null as u001__registered_users__daily_channel_brand__pit,
        null as u002__deregistered_users__daily_channel_brand__pit,
        t001__spend__user_level_daily__sum,
        null as u007__active_users__user_level_daily__uid,
        t003__transactions__user_level_daily__dcount_txn
    from txn

    union all

    select
        tab,
        date,
        channel,
        brand,
        loyalty_plan_company,
        null
            as lc001__successful_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit,
        null as lc004__deleted_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as lc005__successful_loyalty_cards__daily_channel_brand_retailer__count
        ,
        null
            as lc008__deleted_loyalty_cards__daily_channel_brand_retailer__count,
        null as u005__registered_users__daily_channel_brand__count,
        null as u006__deregistered_users__daily_channel_brand__count,
        null as u001__registered_users__daily_channel_brand__pit,
        null as u002__deregistered_users__daily_channel_brand__pit,
        null as t001__spend__user_level_daily__sum,
        u007__active_users__user_level_daily__uid,
        null as t003__transactions__user_level_daily__dcount_txn
    from active_usr

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
            as u003__users_with_a_linked_loyalty_card__daily_channel_brand__pit,
        null as lc004__deleted_loyalty_cards__daily_channel_brand_retailer__pit,
        null
            as lc005__successful_loyalty_cards__daily_channel_brand_retailer__count
        ,
        null
            as lc008__deleted_loyalty_cards__daily_channel_brand_retailer__count,
        u005__registered_users__daily_channel_brand__count,
        u006__deregistered_users__daily_channel_brand__count,
        u001__registered_users__daily_channel_brand__pit,
        u002__deregistered_users__daily_channel_brand__pit,
        null as t001__spend__user_level_daily__sum,
        null as u007__active_users__user_level_daily__uid,
        null as t003__transactions__user_level_daily__dcount_txn
    from users_metrics
)

select *
from metric_select
