/*
Created by:         Anand Bhakta
Created date:       2023-02-02
Last modified by:   
Last modified date: 

Description:
    Table representing all api response time data from the apistats table

Parameters:
    source_object      - SERVICE_DATA.APISTATS
*/
{{ config(materialized="incremental", unique_key="API_ID") }}

with
    all_events as (
        select *
        from {{ ref("stg_service_data__APISTATS") }}
        {% if is_incremental() %}
        where _airbyte_emitted_at >= (select max(inserted_date_time) from {{ this }})
        {% endif %}

    ),
    extract_channel as (
        select
            api_id,
            date_time,
            method,
            path,
            coalesce(
                case when contains(client_ip, '141.92') then 'LLOYDS' else null end,
                case when contains(client_ip, '157.83') then 'BARCLAYS' else null end
            ) as channel,
            response_time,
            status_code,
            sysdate() as inserted_date_time
        from all_events
    )

select *
from extract_channel
