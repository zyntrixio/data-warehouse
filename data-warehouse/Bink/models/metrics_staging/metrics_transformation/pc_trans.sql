/*
Last modified by:   Christopher Mitchell
Last modified date: 2024-04-09

Description:
    
Notes:
    
Parameters:
    source_object       - src_fact_payment_card
                        - src__dim_date
*/


{{
    config(
        materialized = 'incremental',
        unique_key = 'EVENT_ID'
        ) 
}}

with
pc_events as (select * from {{ ref("stg_metrics__fact_payment_card") }}
where

{% if is_incremental() %}
            and payment_card_id in 
            (
                select 
                    payment_card_id 
                from 
                    {{ ref("stg_metrics__fact_payment_card") }}
                where 
                    inserted_date_time >= (select max(inserted_date_time) from {{ this }})
                    )
    {% endif %}
)

select * from pc_events
