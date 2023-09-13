/*
Created by:         Anand Bhakta
Created date:       2023-09-12
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
    where
        date >= (select min(from_date) from lc_events)
        and date <= current_date()
),

count_up_snap as (
    select
        d.date,
        u.loyalty_plan_name,
        u.loyalty_plan_company,
        coalesce(
            sum(
                case
                    when event_type = 'SUCCESS' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_success_state,
        coalesce(
            sum(
                case
                    when event_type = 'FAILED' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_failed_state,
        coalesce(
            sum(
                case
                    when event_type = 'REQUEST' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_pending_state,
        coalesce(
            sum(
                case
                    when event_type = 'REMOVED' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_removed_state,
        coalesce(
            sum(
                case
                    when event_type = 'SUCCESS' and add_journey = 'LINK' then 1
                end
            ),
            0
        ) as link_success_state,
        coalesce(
            sum(
                case
                    when event_type = 'FAILED' and add_journey = 'LINK' then 1
                end
            ),
            0
        ) as link_failed_state,
        coalesce(
            sum(
                case
                    when event_type = 'REQUEST' and add_journey = 'LINK' then 1
                end
            ),
            0
        ) as link_pending_state,
        coalesce(
            sum(
                case
                    when event_type = 'REMOVED' and add_journey = 'LINK' then 1
                end
            ),
            0
        ) as link_removed_state
    from lc_events u
    left join
        dim_date d
        on
            d.date >= date(u.from_date)
            and d.date < coalesce(date(u.to_date), '9999-12-31')
    group by
        d.date, u.loyalty_plan_name, u.loyalty_plan_company
    having date is not null
),

count_up_abs as (
    select
        d.date,
        u.loyalty_plan_name,
        u.loyalty_plan_company,
        coalesce(
            sum(
                case
                    when event_type = 'REQUEST' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_requests,
        coalesce(
            sum(
                case
                    when event_type = 'FAILED' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_fails,
        coalesce(
            sum(
                case
                    when event_type = 'SUCCESS' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_successes,
        coalesce(
            sum(
                case
                    when event_type = 'REMOVED' and add_journey = 'JOIN' then 1
                end
            ),
            0
        ) as join_deletes,
        coalesce(
            sum(
                case
                    when event_type = 'REQUEST' and add_journey = 'LINK' then 1
                end
            ),
            0
        ) as link_requests,
        coalesce(
            sum(
                case
                    when event_type = 'FAILED' and add_journey = 'LINK' then 1
                end
            ),
            0
        ) as link_fails,
        coalesce(
            sum(
                case
                    when event_type = 'SUCCESS' and add_journey = 'LINK' then 1
                end
            ),
            0
        ) as link_successes,
        coalesce(
            sum(
                case
                    when event_type = 'REMOVED' and add_journey = 'LINK' then 1
                end
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
            count(
                distinct case when event_type = 'REQUEST' then u.user_ref end
            ),
            0
        ) as requests_unique_users,
        coalesce(
            count(distinct case when event_type = 'FAILED' then u.user_ref end),
            0
        ) as fails_unique_users,
        coalesce(
            count(
                distinct case when event_type = 'SUCCESS' then u.user_ref end
            ),
            0
        ) as successes_unique_users,
        coalesce(
            count(
                distinct case when event_type = 'REMOVED' then u.user_ref end
            ),
            0
        ) as deletes_unique_users

    from dim_date d
    left join lc_events u on d.date = date(u.from_date)
    group by
        d.date, u.loyalty_plan_name, u.loyalty_plan_company
    having date is not null
),

adding_cumulative_abs as (

    select
        date,
        loyalty_plan_name,
        loyalty_plan_company,
        join_requests,
        join_fails,
        join_successes,
        join_deletes,
        link_requests,
        link_fails,
        link_successes,
        link_deletes,
        sum(join_requests) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as join_requests_cumulative,
        sum(join_fails) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as join_fails_cumulative,
        sum(join_successes) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as join_successes_cumulative,
        sum(join_deletes) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as join_deletes_cumulative,
        sum(link_requests) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as link_requests_cumulative,
        sum(link_fails) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as link_fails_cumulative,
        sum(link_successes) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as link_successes_cumulative,
        sum(link_deletes) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as link_deletes_cumulative,
        join_requests_unique_users,
        join_fails_unique_users,
        join_successes_unique_users,
        join_deletes_unique_users,
        link_requests_unique_users,
        link_fails_unique_users,
        link_successes_unique_users,
        link_deletes_unique_users

    from count_up_abs
),

all_together as (
    select
        coalesce(a.date, s.date) date,
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
        coalesce(a.join_requests_cumulative, 0) as join_requests_cumulative,
        coalesce(a.join_fails_cumulative, 0) as join_fails_cumulative,
        coalesce(a.join_successes_cumulative, 0) as join_successes_cumulative,
        coalesce(a.join_deletes_cumulative, 0) as join_deletes_cumulative,
        coalesce(a.link_requests_cumulative, 0) as link_requests_cumulative,
        coalesce(a.link_fails_cumulative, 0) as link_fails_cumulative,
        coalesce(a.link_successes_cumulative, 0) as link_successes_cumulative,
        coalesce(a.link_deletes_cumulative, 0) as link_deletes_cumulative,
        coalesce(a.join_requests_unique_users, 0) as join_requests_unique_users,
        coalesce(a.join_fails_unique_users, 0) as join_fails_unique_users,
        coalesce(a.join_successes_unique_users, 0)
            as join_successes_unique_users,
        coalesce(a.join_deletes_unique_users, 0) as join_deletes_unique_users,
        coalesce(a.link_requests_unique_users, 0) as link_requests_unique_users,
        coalesce(a.link_fails_unique_users, 0) as link_fails_unique_users,
        coalesce(a.link_successes_unique_users, 0)
            as link_successes_unique_users,
        coalesce(a.link_deletes_unique_users, 0) as link_deletes_unique_users
    from adding_cumulative_abs a
    full outer join
        count_up_snap s
        on
            a.date = s.date
            and a.loyalty_plan_name = s.loyalty_plan_name
),

add_combine_rename as (
    select
        date,
        loyalty_plan_name,
        loyalty_plan_company,
        join_success_state
            as lc033__successful_loyalty_card_joins__daily_retailer__pit,
        join_failed_state
            as lc034__failed_loyalty_card_joins__daily_retailer__pit,
        join_pending_state
            as lc035__requests_loyalty_card_joins__daily_retailer__pit,
        join_removed_state
            as lc036__deleted_loyalty_card_joins__daily_retailer__pit,
        link_success_state
            as lc037__successful_loyalty_card_links__daily_retailer__pit,
        link_failed_state
            as lc038__failed_loyalty_card_links__daily_retailer__pit,
        link_pending_state
            as lc039__requests_loyalty_card_links__daily_retailer__pit,
        link_removed_state
            as lc040__deleted_loyalty_card_links__daily_retailer__pit,
        join_success_state
        + link_success_state
            as lc041__successful_loyalty_cards__daily_retailer__pit,
        join_pending_state
        + link_pending_state
            as lc042__requests_loyalty_cards__daily_retailer__pit,
        join_failed_state
        + link_failed_state
            as lc043__failed_loyalty_cards__daily_retailer__pit,
        link_removed_state
        + join_removed_state
            as lc044__deleted_loyalty_cards__daily_retailer__pit,
        join_requests
            as lc045__requests_loyalty_card_joins__daily_retailer__count,
        join_fails
            as lc046__failed_loyalty_card_joins__daily_retailer__count,
        join_successes
            as lc047__successful_loyalty_card_joins__daily_retailer__count
        ,
        join_deletes
            as lc048__deleted_loyalty_card_joins__daily_retailer__count,
        link_requests
            as lc049__requests_loyalty_card_links__daily_retailer__count,
        link_fails
            as lc050__failed_loyalty_card_links__daily_retailer__count,
        link_successes
            as lc051__successful_loyalty_card_links__daily_retailer__count
        ,
        link_deletes
            as lc052__deleted_loyalty_card_links__daily_retailer__count,
        join_requests
        + link_requests
            as lc053__requests_loyalty_cards__daily_retailer__count,
        join_fails
        + link_fails
            as lc054__failed_loyalty_cards__daily_retailer__count,
        join_successes
        + link_successes
            as lc055__successful_loyalty_cards__daily_retailer__count,
        join_deletes
        + link_deletes
            as lc056__deleted_loyalty_cards__daily_retailer__count,
        join_requests_unique_users
            as lc057__requests_loyalty_card_joins__daily_retailer__dcount_user
        ,
        join_fails_unique_users
            as lc058__failed_loyalty_card_joins__daily_retailer__dcount_user
        ,
        join_successes_unique_users
            as lc059__successful_loyalty_card_joins__daily_retailer__dcount_user
        ,
        join_deletes_unique_users
            as lc060__deleted_loyalty_card_joins__daily_retailer__dcount_user
        ,
        link_requests_unique_users
            as lc061__requests_loyalty_card_links__daily_retailer__dcount_user
        ,
        link_fails_unique_users
            as lc062__failed_loyalty_card_links__daily_retailer__dcount_user
        ,
        link_successes_unique_users
            as lc063__successful_loyalty_card_links__daily_retailer__dcount_user
        ,
        link_deletes_unique_users
            as lc064__deleted_loyalty_card_links__daily_retailer__dcount_user,
        join_requests_cumulative
            as lc065__requests_loyalty_card_joins__daily_retailer__csum,
        join_fails_cumulative
            as lc066__failed_loyalty_card_joins__daily_retailer__csum,
        join_successes_cumulative
            as lc067__successful_loyalty_card_joins__daily_retailer__csum
        ,
        join_deletes_cumulative
            as lc068__deleted_loyalty_card_joins__daily_retailer__csum,
        link_requests_cumulative
            as lc069__requests_loyalty_card_links__daily_retailer__csum,
        link_fails_cumulative
            as lc070__failed_loyalty_card_links__daily_retailer__csum,
        link_successes_cumulative
            as lc071__successful_loyalty_card_links__daily_retailer__csum
        ,
        link_deletes_cumulative
            as lc072__deleted_loyalty_card_links__daily_retailer__csum
        

    from all_together
)

select *
from add_combine_rename
