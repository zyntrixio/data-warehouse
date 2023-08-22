/*
Created by:         Anand Bhakta
Created date:       2023-02-05
Last modified by:   
Last modified date: 

Description:
    Rewrite of the LL table lc_joins_links_snapshot and lc_joins_links containing both snapshot and daily absolute data of all link and join journeys split by RETAILER.
Notes:
    This code can be made more efficient if the start is pushed to the trans__lbg_user code and that can be the source for the majority of the dashboards including user_loyalty_plan_snapshot and user_with_loyalty_cards
Parameters:
    source_object       - src__fact_lc_add
                        - src__fact_lc_removed
                        - src__dim_loyalty_card
                        - src__dim_date
*/
with
    lc_events as (select * from {{ ref("lc_trans") }}),
    dim_date as (
        select *
        from {{ ref("stg_metrics__dim_date") }}
        where date >= (select min(from_date) from lc_events) and date <= current_date()
    ),
    count_up_snap as (
        select
            d.date,
            u.channel,
            u.brand,
            u.loyalty_plan_name,
            u.loyalty_plan_company,
            coalesce(
                sum(
                    case when event_type = 'SUCCESS' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_success_state,
            coalesce(
                sum(
                    case when event_type = 'FAILED' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_failed_state,
            coalesce(
                sum(
                    case when event_type = 'REQUEST' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_pending_state,
            coalesce(
                sum(
                    case when event_type = 'REMOVED' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_removed_state,
            coalesce(
                sum(
                    case when event_type = 'SUCCESS' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_success_state,
            coalesce(
                sum(
                    case when event_type = 'FAILED' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_failed_state,
            coalesce(
                sum(
                    case when event_type = 'REQUEST' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_pending_state,
            coalesce(
                sum(
                    case when event_type = 'REMOVED' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_removed_state
        from lc_events u
        left join
            dim_date d
            on d.date >= date(u.from_date)
            and d.date < coalesce(date(u.to_date), '9999-12-31')
        group by d.date, u.brand, u.channel, u.loyalty_plan_name, u.loyalty_plan_company
        having date is not null
    ),
    count_up_abs as (
        select
            d.date,
            u.channel,
            u.brand,
            u.loyalty_plan_name,
            u.loyalty_plan_company,
            coalesce(
                sum(
                    case when event_type = 'REQUEST' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_requests,
            coalesce(
                sum(
                    case when event_type = 'FAILED' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_fails,
            coalesce(
                sum(
                    case when event_type = 'SUCCESS' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_successes,
            coalesce(
                sum(
                    case when event_type = 'REMOVED' and add_journey = 'JOIN' then 1 end
                ),
                0
            ) as join_deletes,
            coalesce(
                sum(
                    case when event_type = 'REQUEST' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_requests,
            coalesce(
                sum(
                    case when event_type = 'FAILED' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_fails,
            coalesce(
                sum(
                    case when event_type = 'SUCCESS' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_successes,
            coalesce(
                sum(
                    case when event_type = 'REMOVED' and add_journey = 'LINK' then 1 end
                ),
                0
            ) as link_deletes,
            coalesce(
                count(
                    distinct case
                        when event_type = 'REQUEST' and add_journey = 'JOIN'
                        then u.user_ref
                    end
                ),
                0
            ) as join_requests_unique_users,
            coalesce(
                count(
                    distinct case
                        when event_type = 'FAILED' and add_journey = 'JOIN'
                        then u.user_ref
                    end
                ),
                0
            ) as join_fails_unique_users,
            coalesce(
                count(
                    distinct case
                        when event_type = 'SUCCESS' and add_journey = 'JOIN'
                        then u.user_ref
                    end
                ),
                0
            ) as join_successes_unique_users,
            coalesce(
                count(
                    distinct case
                        when event_type = 'REMOVED' and add_journey = 'JOIN'
                        then u.user_ref
                    end
                ),
                0
            ) as join_deletes_unique_users,
            coalesce(
                count(
                    distinct case
                        when event_type = 'REQUEST' and add_journey = 'LINK'
                        then u.user_ref
                    end
                ),
                0
            ) as link_requests_unique_users,
            coalesce(
                count(
                    distinct case
                        when event_type = 'FAILED' and add_journey = 'LINK'
                        then u.user_ref
                    end
                ),
                0
            ) as link_fails_unique_users,
            coalesce(
                count(
                    distinct case
                        when event_type = 'SUCCESS' and add_journey = 'LINK'
                        then u.user_ref
                    end
                ),
                0
            ) as link_successes_unique_users,
            coalesce(
                count(
                    distinct case
                        when event_type = 'REMOVED' and add_journey = 'LINK'
                        then u.user_ref
                    end
                ),
                0
            ) as link_deletes_unique_users,
            coalesce(
                count(distinct case when event_type = 'REQUEST' then u.user_ref end), 0
            ) as requests_unique_users,
            coalesce(
                count(distinct case when event_type = 'FAILED' then u.user_ref end), 0
            ) as fails_unique_users,
            coalesce(
                count(distinct case when event_type = 'SUCCESS' then u.user_ref end), 0
            ) as successes_unique_users,
            coalesce(
                count(distinct case when event_type = 'REMOVED' then u.user_ref end), 0
            ) as deletes_unique_users

        from lc_events u
        left join dim_date d on d.date = date(u.from_date)
        group by d.date, u.brand, u.channel, u.loyalty_plan_name, u.loyalty_plan_company
        having date is not null
    ),
    all_together as (
        select
            coalesce(a.date, s.date) date,
            coalesce(a.brand, s.brand) brand,
            coalesce(a.channel, s.channel) channel,
            coalesce(a.loyalty_plan_name, s.loyalty_plan_name) loyalty_plan_name,
            coalesce(
                a.loyalty_plan_company, s.loyalty_plan_company
            ) loyalty_plan_company,
            coalesce(s.join_success_state, 0) as join_success_state,
            coalesce(s.join_failed_state, 0) as join_failed_state,
            coalesce(s.join_pending_state, 0) as join_pending_state,
            coalesce(s.join_removed_state, 0) as join_removed_state,
            coalesce(s.link_success_state, 0) as link_success_state,
            coalesce(s.link_failed_state, 0) as link_failed_state,
            coalesce(s.link_pending_state, 0) as link_pending_state,
            coalesce(s.link_removed_state, 0) as link_removed_state,
            coalesce(a.join_requests, 0) as join_requests,
            coalesce(a.join_fails, 0) as join_fails,
            coalesce(a.join_successes, 0) as join_successes,
            coalesce(a.join_deletes, 0) as join_deletes,
            coalesce(a.link_requests, 0) as link_requests,
            coalesce(a.link_fails, 0) as link_fails,
            coalesce(a.link_successes, 0) as link_successes,
            coalesce(a.link_deletes, 0) as link_deletes,
            coalesce(a.join_requests_unique_users, 0) as join_requests_unique_users,
            coalesce(a.join_fails_unique_users, 0) as join_fails_unique_users,
            coalesce(a.join_successes_unique_users, 0) as join_successes_unique_users,
            coalesce(a.join_deletes_unique_users, 0) as join_deletes_unique_users,
            coalesce(a.link_requests_unique_users, 0) as link_requests_unique_users,
            coalesce(a.link_fails_unique_users, 0) as link_fails_unique_users,
            coalesce(a.link_successes_unique_users, 0) as link_successes_unique_users,
            coalesce(a.link_deletes_unique_users, 0) as link_deletes_unique_users
        from count_up_abs a
        full outer join
            count_up_snap s
            on a.date = s.date
            and a.brand = s.brand
            and a.loyalty_plan_name = s.loyalty_plan_name
    ),
    add_combine_rename as (
        select
            date,
            channel,
            brand,
            loyalty_plan_name,
            loyalty_plan_company,
            join_success_state
            as lc029__successful_loyalty_card_joins__daily_channel_brand_retailer__pit,
            join_failed_state
            as lc031__failed_loyalty_card_joins__daily_channel_brand_retailer__pit,
            join_pending_state
            as lc030__requests_loyalty_card_joins__daily_channel_brand_retailer__pit,
            join_removed_state
            as lc032__deleted_loyalty_card_joins__daily_channel_brand_retailer__pit,
            link_success_state
            as lc025__successful_loyalty_card_links__daily_channel_brand_retailer__pit,
            link_failed_state
            as lc027__failed_loyalty_card_links__daily_channel_brand_retailer__pit,
            link_pending_state
            as lc026__requests_loyalty_card_links__daily_channel_brand_retailer__pit,
            link_removed_state
            as lc028__deleted_loyalty_card_links__daily_channel_brand_retailer__pit,
            join_success_state
            + link_success_state
            as lc001__successful_loyalty_cards__daily_channel_brand_retailer__pit,
            join_pending_state
            + link_pending_state
            as lc002__requests_loyalty_cards__daily_channel_brand_retailer__pit,
            join_failed_state
            + link_failed_state
            as lc003__failed_loyalty_cards__daily_channel_brand_retailer__pit,
            link_removed_state
            + join_removed_state
            as lc004__deleted_loyalty_cards__daily_channel_brand_retailer__pit,
            join_requests
            as lc014__requests_loyalty_card_joins__daily_channel_brand_retailer__count,
            join_fails
            as lc015__failed_loyalty_card_joins__daily_channel_brand_retailer__count,
            join_successes
            as lc013__successful_loyalty_card_joins__daily_channel_brand_retailer__count
            ,
            join_deletes
            as lc016__deleted_loyalty_card_joins__daily_channel_brand_retailer__count,
            link_requests
            as lc010__requests_loyalty_card_links__daily_channel_brand_retailer__count,
            link_fails
            as lc011__failed_loyalty_card_links__daily_channel_brand_retailer__count,
            link_successes
            as lc009__successful_loyalty_card_links__daily_channel_brand_retailer__count
            ,
            link_deletes
            as lc012__deleted_loyalty_card_links__daily_channel_brand_retailer__count,
            join_requests
            + link_requests
            as lc006__requests_loyalty_cards__daily_channel_brand_retailer__count,
            join_fails
            + link_fails
            as lc007__failed_loyalty_cards__daily_channel_brand_retailer__count,
            join_successes
            + link_successes
            as lc005__successful_loyalty_cards__daily_channel_brand_retailer__count,
            join_deletes
            + link_deletes
            as lc008__deleted_loyalty_cards__daily_channel_brand_retailer__count,
            join_requests_unique_users
            as lc022__requests_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
            ,
            join_fails_unique_users
            as lc023__failed_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
            ,
            join_successes_unique_users
            as lc021__successful_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
            ,
            join_deletes_unique_users
            as lc024__deleted_loyalty_card_joins__daily_channel_brand_retailer__dcount_user
            ,
            link_requests_unique_users
            as lc018__requests_loyalty_card_links__daily_channel_brand_retailer__dcount_user
            ,
            link_fails_unique_users
            as lc019__failed_loyalty_card_links__daily_channel_brand_retailer__dcount_user
            ,
            link_successes_unique_users
            as lc017__successful_loyalty_card_links__daily_channel_brand_retailer__dcount_user
            ,
            link_deletes_unique_users
            as lc020__deleted_loyalty_card_links__daily_channel_brand_retailer__dcount_user

        from all_together
    )

select *
from add_combine_rename
