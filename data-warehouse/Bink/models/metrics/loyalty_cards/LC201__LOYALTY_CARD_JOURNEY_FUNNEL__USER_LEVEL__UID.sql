/*
Created by:         Anand Bhakta
Created date:       2023-06-27
Last modified by:
Last modified date:

Description:
    Sankey funnel data for lloyds to be joined with the sankey model table
Parameters:
    source_object      - src__fact_lc_add
*/
with
lc as (
    select *
    from {{ ref("stg_metrics__fact_lc") }}
    where event_type not in ('REQUEST', 'REMOVED')
),

lc_group as (
    select
        external_user_ref,
        user_id,
        coalesce(nullif(external_user_ref, ''), user_id) as user_ref,
        event_date_time,
        brand,
        channel,
        case
            when lc.auth_type in ('JOIN', 'REGISTER') then 'JOIN' else 'LINK'
        end as auth_type,
        event_type,
        loyalty_card_id,
        loyalty_plan_name,
        loyalty_plan_company,
        sum(case when event_type = 'SUCCESS' then 1 else 0 end) over (
            partition by
                coalesce(nullif(external_user_ref, ''), user_id),
                loyalty_plan_name
            order by event_date_time asc
        ) as lc_group
    from lc
),

lc_group2 as (
    select
        user_ref,
        event_date_time,
        brand,
        channel,
        auth_type,
        event_type,
        loyalty_card_id,
        loyalty_plan_name,
        loyalty_plan_company,
        case
            when event_type = 'SUCCESS' then lc_group - 1 else lc_group
        end as lc_group
    from lc_group
),

modify_fields as (
    select
        user_ref,
        event_date_time,
        brand,
        channel,
        auth_type,
        first_value(auth_type) over (
            partition by user_ref, loyalty_plan_name, lc_group
            order by event_date_time asc
        ) as org_auth_type,
        first_value(event_date_time) over (
            partition by user_ref, loyalty_plan_name, lc_group
            order by event_date_time asc
        ) as start_time,
        lag(auth_type) over (
            partition by user_ref, loyalty_plan_name, lc_group
            order by event_date_time asc
        ) as prev_auth_type,
        row_number() over (
            partition by user_ref, loyalty_plan_name, lc_group
            order by event_date_time asc
        ) as attempt,
        last_value(concat(auth_type, ' ', event_type)) over (
            partition by user_ref, loyalty_plan_name, lc_group
            order by event_date_time asc
        ) as last_val,
        concat(auth_type, ' ', event_type) as event_type,
        loyalty_card_id,
        loyalty_plan_name,
        loyalty_plan_company,
        lc_group
    from lc_group2
),

select_fields as (
    select
        loyalty_plan_name,
        loyalty_plan_company,
        brand,
        channel,
        concat(user_ref, ' ', loyalty_plan_name, ' ', lc_group) as id,
        org_auth_type,
        1 as size,
        'link' as link,
        start_time,
        attempt,
        case when attempt = 5 then last_val else event_type end as event_type
    from modify_fields

),

pivot as (
    select *
    from select_fields pivot (min(event_type) for attempt in (1, 2, 3, 4, 5))
)

select *
from pivot
