/*
Created by:         Anand Bhakta
Created date:       2023-07-12
Last modified by:
Last modified date:

Description:
    Set up of error to and from data for loyalty card error statuses excluding pending and active
Parameters:
    source_object       - src__fact_lc_status_change
                        - src__lookup_status_mapping
*/
with
pll_events as (select * from {{ ref("stg_metrics__pll_link_status_change") }}
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
    {% for retailor, start_date in var("retailor_start_date").items() %}
        ((loyalty_plan_company = '{{retailor}}' and event_date_time >= '{{start_date}}') or loyalty_plan_company != '{{retailor}}')
    {%- if not loop.last %} and {% endif -%}
    {% endfor %}
), 

from_to_dates as (
    select
        coalesce(nullif(external_user_ref, ''), user_id) as user_ref,
        concat(
            coalesce(nullif(external_user_ref, ''), user_id),
            loyalty_plan_company
        ) as lc_user_ref,
        channel,
        brand,
        loyalty_card_id,
        loyalty_plan_company,
        loyalty_plan_name,
        payment_account_id,
        event_date_time as from_date,
        lead(event_date_time, 1) over (
            partition by loyalty_card_id, payment_account_id
            order by event_date_time asc
        ) as to_date,
        to_status as status,
        to_status = 'ACTIVE' as active_link
    from pll_events
)

select *
from from_to_dates
