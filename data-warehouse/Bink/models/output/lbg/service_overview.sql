/*
Created by:         Anand Bhakta
Created date:       2023-06-30
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-23

Description:
    Datasource to produce lloyds mi dashboard - service_overview
Parameters:
    source_object       - stg_metrics__apistats
                        - stg_metrics__service_management
                        - trans__trans__daily_user_level
*/
with
apistats as (
    select
        *,
        'API' as tab
    from {{ ref("stg_metrics__apistats") }}
    where channel = 'LLOYDS'
),

service as (
    select
        *,
        'SERVICE' as tab
    from {{ ref("stg_metrics__service_management") }}
    where channel = 'LLOYDS'
),

trans as (
    select
        *,
        'TRANS' as tab
    from {{ ref("trans__trans__daily_user_level") }}
    where channel = 'LLOYDS'
),

combine as (
    select
        tab,
        date,
        channel,
        api_id,
        method,
        path,
        response_time,
        status_code,
        null as ticket_id,
        null as mi,
        null as service,
        null as sla_breached,
        null as t002__active_users__user_level_daily__uid
    from apistats

    union all

    select
        tab,
        date,
        channel,
        null as api_id,
        null as method,
        null as path,
        null as response_time,
        null as status_code,
        ticket_id,
        mi,
        service,
        sla_breached,
        null as t002__active_users__user_level_daily__uid
    from service

    union all

    select
        tab,
        date,
        channel,
        null as api_id,
        null as method,
        null as path,
        null as response_time,
        null as status_code,
        null as ticket_id,
        null as mi,
        null as service,
        null as sla_breached,
        t002__active_users__user_level_daily__uid
    from trans
)

select *
from combine
