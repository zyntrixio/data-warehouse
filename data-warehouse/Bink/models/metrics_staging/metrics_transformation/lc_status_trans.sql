/*
Created by:         Anand Bhakta
Created date:       2023-05-09
Last modified by:
Last modified date: 

Description:
    Set up of error to and from data for loyalty card error statuses excluding pending and active
Parameters:
    source_object       - src__fact_lc_status_change
                        - src__lookup_status_mapping
*/
with
    lc_sc as (select * from {{ ref("stg_metrics__fact_lc_status_change") }}),
    lc_lookup as (select * from {{ ref("src__lookup_status_mapping") }}),
    event_ordering as (  -- Get Future And previous events per LC & User
        select
            event_date_time as status_start_time,
            to_status_id as status_id,
            to_status as status_description,
            channel,
            loyalty_card_id,
            user_id,
            external_user_ref,
            coalesce(nullif(external_user_ref, ''), user_id) as user_ref,
            brand,
            loyalty_plan_name,
            loyalty_plan_company,
            lead(event_date_time, 1) over (
                partition by
                    loyalty_plan_name, coalesce(nullif(external_user_ref, ''), user_id)
                order by event_date_time
            ) as status_end_time,
            lead(to_status_id, 1) over (
                partition by
                    loyalty_plan_name, coalesce(nullif(external_user_ref, ''), user_id)
                order by event_date_time
            ) as next_status_id,
            lag(to_status_id, 1) over (
                partition by
                    loyalty_plan_name, coalesce(nullif(external_user_ref, ''), user_id)
                order by event_date_time
            ) as prev_status_id
        from lc_sc
    ),
    join_status_types as (  -- Join in lookup table to determine which status' are errors
        select
            lc.*,
            lcl.status_type,
            lcl.status_group,
            lcl.status_rollup,
            lcl_next.status_type as next_status_type
        from event_ordering lc
        left join lc_lookup lcl on lc.status_id = lcl.code
        left join lc_lookup lcl_prev on lc.prev_status_id = lcl_prev.code
        left join lc_lookup lcl_next on lc.next_status_id = lcl_next.code
        where
            (lcl.status_type != 'Active' or lcl_prev.status_type != 'Active')  -- Ignore Active -> Active
            and (lcl.status_type != 'Pending' or lcl_prev.status_type != 'Pending')  -- Ignore Pending -> Pending
    ),
    add_metrics as (  -- Add useful reporting metrics & Calculate time differences between subsequent events
        select
            *,
            case
                when prev_status_id is not null and prev_status_id = status_id
                then true
                else false
            end as repeated_status,
            case
                when status_type = 'Error' and next_status_type in ('Success')
                then true
                else false
            end as to_resolved,
            case
                when
                    coalesce(
                        sum(case when status_type = 'Success' then 1 else 0 end) over (
                            partition by loyalty_plan_name, user_ref
                            order by status_start_time
                            rows between 1 following and unbounded following
                        ),
                        0
                    )
                    >= 1
                then true
                else false
            end as is_resolved,
            case when status_end_time is null then true else false end as is_final_state
            ,
            datediff(day, status_start_time, status_end_time) as timediff_days,
            datediff(hour, status_start_time, status_end_time) as timediff_hours,
            datediff(min, status_start_time, status_end_time) as timediff_mins,
            datediff(sec, status_start_time, status_end_time) as timediff_seconds,
            datediff(
                millisecond, status_start_time, status_end_time
            ) as timediff_milliseconds
        from join_status_types
    ),
    filter_non_error_events as (  -- Filter out all non Error events
        select
            status_id,
            status_description,
            status_group,
            status_rollup,
            status_type,
            user_id,
            external_user_ref,
            user_ref,
            channel,
            brand,
            loyalty_card_id,
            loyalty_plan_name,
            loyalty_plan_company,
            repeated_status,
            to_resolved,
            is_resolved,
            is_final_state,
            status_start_time,
            status_end_time,
            timediff_days,
            timediff_hours,
            timediff_mins,
            timediff_seconds,
            timediff_milliseconds
        from add_metrics lc
    -- WHERE STATUS_TYPE = 'Error'
    )

select *
from filter_non_error_events
