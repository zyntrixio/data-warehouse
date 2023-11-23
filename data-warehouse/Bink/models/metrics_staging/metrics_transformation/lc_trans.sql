/*
Created by:         Anand Bhakta
Created date:       2023-05-05
Last modified by:   Christopher Mitchell
Last modified date: 2023-11-23

Description:
    Rewrite of the LL table lc_joins_links_snapshot and lc_joins_links containing both snapshot and daily absolute data of all link and join journeys split by merchant.
Notes:
    This code can be made more efficient if the start is pushed to the trans__lbg_user code and that can be the source for the majority of the dashboards including user_loyalty_plan_snapshot and user_with_loyalty_cards
Parameters:
    source_object       - src__fact_lc
                        - src__dim_date
*/
with
lc_events as (select * from {{ ref("stg_metrics__fact_lc") }}
where
{#
        {%- for retailor, exclusions in var("retailor_exclusion_lists").items() %}
        {%- for field, list in exclusions.items() %}
        ((loyalty_plan_company = '{{retailor}}' and  {{field}} not in (
                {%- for value in list -%}
                    '{{value}}'
                {%- if not loop.last %} , {% endif -%}
                {%- endfor -%}
            ) or loyalty_plan_company != '{{retailor}}')
        {%- if not loop.last %} and {% endif -%}
        {% endfor %}
    {%- if not loop.last %} and {% endif -%}
    {% endfor %}) -- ridiculous solution for excluding values per merchant
#}
    {% for retailor, dates in var("retailor_live_dates").items() %}
        ((loyalty_plan_company = '{{retailor}}' and event_date_time >= '{{dates[0]}}' and event_date_time <= '{{dates[1]}}') or loyalty_plan_company != '{{retailor}}')
    {%- if not loop.last %} and {% endif -%}
    {% endfor %}


), 

transforming_deletes as (
    select
        event_date_time,
        user_id,
        channel,
        brand,
        coalesce(nullif(external_user_ref, ''), user_id) as user_ref,
        loyalty_card_id,
        loyalty_plan_name,
        loyalty_plan_company,
        event_type,
        case
        event_type
            when 'REMOVED'
                then
                    lag(auth_type, 1) over (
                        partition by user_ref, loyalty_card_id
                        order by event_date_time asc
                    )
            else auth_type
        end as auth_type,
        lag(event_type, 1) over (
            partition by user_ref, loyalty_card_id order by event_date_time asc
        ) as prev_event,
        consent_slug,
        consent_response
    from lc_events
    qualify not (event_type = 'REMOVED' and prev_event != 'SUCCESS')
),

to_from_dates as (
    select
        user_id,
        channel,
        brand,
        user_ref,
        loyalty_card_id,
        coalesce(
            case when auth_type in ('ADD AUTH', 'AUTH', 'ADD TRUSTED') then 'LINK' end,
            case when auth_type in ('JOIN', 'REGISTER') then 'JOIN' end
        ) as add_journey,
        event_type,
        loyalty_plan_name,
        loyalty_plan_company,
        event_type as from_event,
        event_date_time as from_date,
        coalesce(
            lead(event_date_time, 1) over (
                partition by user_ref, loyalty_plan_name
                order by event_date_time
            ),
            case loyalty_plan_company
                {% for retailor, dates in var("retailor_live_dates").items() %}
                     when '{{retailor}}' then '{{dates[1]}}'
                {% endfor %}
            else current_timestamp
            end
        ) as to_date,
        consent_slug,
        consent_response
    from transforming_deletes
)

select *
from to_from_dates
