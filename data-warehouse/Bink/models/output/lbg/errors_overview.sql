/*
Created by:         Anand Bhakta
Created date:       2023-06-07
Last modified by:   
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - loyalty_cards_overview
Parameters:
    source_object       - lc__errors__daily_user_level
                        - lc__links_joins__daily_retailer_channel
*/
with
    lc_errors as (
        select *, 'ERRORS' as tab
        from {{ ref("lc__errors__daily_status_rollup_user_level") }}
        where
            channel = 'LLOYDS'
            and status_rollup != 'System Issue'
            and loyalty_plan_company not in ('Loyalteas', 'Bink Sweet Shop')
    ),
    lc_core as (
        select *, 'LC_LINKS_JOINS' as tab
        from {{ ref("lc__links_joins__daily_retailer_channel") }}
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
            status_rollup,
            lc101__error_loyalty_cards__daily_user_level__uid,
            lc102__resolved_error_loyalty_cards__daily_user_level__uid,
            lc103__error_visits__daily_user_level__count,
            lc104__unresolved_error_loyalty_cards__daily_user_level__uid,
            null as lc006__requests_loyalty_cards__daily_channel_brand_retailer__count,
            null as lc007__failed_loyalty_cards__daily_channel_brand_retailer__count
        from lc_errors

        union all

        select
            date,
            tab,
            channel,
            brand,
            loyalty_plan_company,
            null as status_rollup,
            null as lc101__error_loyalty_cards__daily_user_level__uid,
            null as lc102__resolved_error_loyalty_cards__daily_user_level__uid,
            null as lc103__error_visits__daily_user_level__count,
            null as lc104__unresolved_error_loyalty_cards__daily_user_level__uid,
            lc006__requests_loyalty_cards__daily_channel_brand_retailer__count,
            lc007__failed_loyalty_cards__daily_channel_brand_retailer__count
        from lc_core
    )

select *
from combine
