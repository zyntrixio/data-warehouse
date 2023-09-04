/*
Created by:         Christopher Mitchell
Created date:       2023-07-03
Last modified by:
Last modified date:

Description:
    todo
Parameters:
    source_object       - src__fact_lc_add
*/
with
lc_events as (select * from {{ ref("lc_trans") }}),

dim_date as (
    select distinct
        start_of_month,
        end_of_month
    from {{ ref("dim_date") }}
    where
        date >= (select min(from_date) from lc_events)
        and date <= current_date()
),

count_up_snap as (
    select
        d.start_of_month as date,
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
            d.end_of_month >= date(u.from_date)
            and d.end_of_month < coalesce(date(u.to_date), '9999-12-31')
    group by d.start_of_month, u.loyalty_plan_name, u.loyalty_plan_company
    having date is not null
),

count_up_abs as (
    select
        d.start_of_month as date,
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
                    when
                        event_type = 'SUCCESS'
                        and add_journey = 'JOIN'
                        and consent_response
                        then 1
                end
            ),
            0
        ) as join_successes_mrkt_opt_in,
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

    from lc_events u
    left join dim_date d on d.start_of_month = date_trunc('month', u.from_date)
    group by d.start_of_month, u.loyalty_plan_name, u.loyalty_plan_company
    having start_of_month is not null
),

adding_cumulative_abs as (

    select
        date,
        loyalty_plan_name,
        loyalty_plan_company,
        join_requests,
        join_fails,
        join_successes,
        join_successes_mrkt_opt_in,
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
        sum(join_successes_mrkt_opt_in) over (
            partition by loyalty_plan_company, loyalty_plan_name
            order by date asc
        ) as join_successes_mrkt_opt_in_cumulative,
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
        coalesce(a.join_successes_mrkt_opt_in, 0) as join_successes_mrkt_opt_in,
        coalesce(a.join_deletes, 0) as join_deletes,
        coalesce(a.link_requests, 0) as link_requests,
        coalesce(a.link_fails, 0) as link_fails,
        coalesce(a.link_successes, 0) as link_successes,
        coalesce(a.link_deletes, 0) as link_deletes,
        coalesce(a.join_requests_cumulative, 0) as join_requests_cumulative,
        coalesce(a.join_fails_cumulative, 0) as join_fails_cumulative,
        coalesce(a.join_successes_cumulative, 0) as join_successes_cumulative,
        coalesce(
            a.join_successes_mrkt_opt_in_cumulative, 0
        ) as join_successes_mrkt_opt_in_cumulative,
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
        + link_success_state
            as lc335__successful_loyalty_cards__monthly_retailer__pit,
        join_pending_state
        + link_pending_state
            as lc336__requests_loyalty_cards__monthly_retailer__pit
        ,
        join_failed_state
        + link_failed_state
            as lc337__failed_loyalty_cards__monthly_retailer__pit,
        link_removed_state
        + join_removed_state
            as lc338__deleted_loyalty_cards__monthly_retailer__pit,
        link_success_state
            as lc359__successful_loyalty_card_links__monthly_retailer__pit,
        link_pending_state
            as lc360__requests_loyalty_card_links__monthly_retailer__pit,
        link_failed_state
            as lc361__failed_loyalty_card_links__monthly_retailer__pit
        ,
        link_removed_state
            as lc362__deleted_loyalty_card_links__monthly_retailer__pit,
        join_success_state
            as lc363__successful_loyalty_card_joins__monthly_retailer__pit,
        join_pending_state
            as lc364__requests_loyalty_card_joins__monthly_retailer__pit,
        join_failed_state
            as lc365__failed_loyalty_card_joins__monthly_retailer__pit
        ,
        join_removed_state
            as lc366__deleted_loyalty_card_joins__monthly_retailer__pit,
        join_successes
        + link_successes
            as lc339__successful_loyalty_cards__monthly_retailer__count
        ,
        join_requests
        + link_requests
            as lc340__requests_loyalty_cards__monthly_retailer__count,
        join_fails
        + link_fails as lc341__failed_loyalty_cards__monthly_retailer__count,
        join_deletes
        + link_deletes as lc342__deleted_loyalty_cards__monthly_retailer__count,
        link_successes
            as lc343__successful_loyalty_card_links__monthly_retailer__count,
        link_requests
            as lc344__requests_loyalty_card_links__monthly_retailer__count
        ,
        link_fails as lc345__failed_loyalty_card_links__monthly_retailer__count,
        link_deletes
            as lc346__deleted_loyalty_card_links__monthly_retailer__count,
        join_successes
            as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        join_requests
            as lc348__successful_loyalty_card_joins__monthly_retailer__count,
        join_fails
            as lc349__requests_loyalty_card_joins__monthly_retailer__count,
        join_deletes
            as lc350__failed_loyalty_card_joins__monthly_retailer__count,
        join_successes_cumulative
            as lc375__successful_loyalty_card_links__monthly_retailer__csum,
        join_requests_cumulative
            as lc376__requests_loyalty_card_links__monthly_retailer__csum,
        join_fails_cumulative
            as lc377__failed_loyalty_card_links__monthly_retailer__csum,
        join_deletes_cumulative
            as lc378__deleted_loyalty_card_links__monthly_retailer__csum,
        link_successes_cumulative
            as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        link_requests_cumulative
            as lc380__requests_loyalty_card_joins__monthly_retailer__csum,
        link_fails_cumulative
            as lc381__failed_loyalty_card_joins__monthly_retailer__csum,
        link_deletes_cumulative
            as lc382__deleted_loyalty_card_joins__monthly_retailer__csum,
        join_successes_unique_users
            as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        join_requests_unique_users
            as lc352__requests_loyalty_card_links__monthly_retailer__dcount_user,
        join_fails_unique_users
            as lc353__failed_loyalty_card_links__monthly_retailer__dcount_user,
        join_deletes_unique_users
            as lc354__deleted_loyalty_card_links__monthly_retailer__dcount_user,
        link_successes_unique_users
            as lc355__successful_loyalty_card_joins__monthly_retailer__dcount_user,
        link_requests_unique_users
            as lc356__requests_loyalty_card_joins__monthly_retailer__dcount_user,
        link_fails_unique_users
            as lc357__failed_loyalty_card_joins__monthly_retailer__dcount_user,
        link_deletes_unique_users
            as lc358__deleted_loyalty_card_joins__monthly_retailer__dcount_user,
        join_successes_mrkt_opt_in
            as lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        DIV0(join_successes_mrkt_opt_in,join_successes)
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        join_successes_mrkt_opt_in_cumulative
            as lc332__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__csum

    from all_together
)

select *
from add_combine_rename
