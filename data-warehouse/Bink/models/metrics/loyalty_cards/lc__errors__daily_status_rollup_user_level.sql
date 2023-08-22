/*
Created by:         Anand Bhakta
Created date:       2023-06-07
Last modified by:   
Last modified date: 

Description:
    data source for errors broken down daily and by user / status rollup. 
Parameters:
    source_object       - lc__errors__daily_user_level
                        - lc__links_joins__daily_retailer_channel
*/
with
    lc_errors as (
        select * from {{ ref("lc_status_trans") }} where status_type = 'Error'
    ),
    errors_aggregate as (
        select
            date(status_start_time) as date,
            channel,
            brand,
            loyalty_plan_company,
            status_rollup,
            is_resolved,
            concat(user_ref, loyalty_plan_company) as lc_user_ref
        from lc_errors
    ),
    errors_metrics as (
        select
            date,
            channel,
            brand,
            loyalty_plan_company,
            status_rollup,
            max(lc_user_ref) as lc101__error_loyalty_cards__daily_user_level__uid,
            max(
                case when is_resolved then lc_user_ref end
            ) as lc102__resolved_error_loyalty_cards__daily_user_level__uid,
            max(
                case when not (is_resolved) then lc_user_ref end
            ) as lc104__unresolved_error_loyalty_cards__daily_user_level__uid,
            count(*) as lc103__error_visits__daily_user_level__count
        from errors_aggregate
        group by date, loyalty_plan_company, channel, brand, status_rollup, lc_user_ref
    )

select *
from errors_metrics
