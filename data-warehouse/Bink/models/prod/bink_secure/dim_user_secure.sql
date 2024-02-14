/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   Sam Pibworth
Last modified date: 2022-04-22

Description:
    The DIM user table, relating to hermes.user

Parameters:
    ref_object      - stg_hermes__user
*/

{{
    config(
        alias="dim_user",
        materialized="incremental",
        unique_key="user_id",
        merge_update_columns = ['deleted_time', 'last_active', 'is_active', 'updated_date_time'] 
    )
}}

--merge update columns above work because the only possible events that come in late are deletes each user can only have 2 events (create / delete)

with
user_events as (
    select *
    from {{ ref("fact_user_secure") }}
        {% if is_incremental() %}
            where inserted_date_time
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %} --bring in new rows only - no need for union due to there only being delete updates see above
)



,user_refresh as (select * from {{ref('fact_wallet_refresh_secure')}}
    {% if is_incremental() %}
            where inserted_date_time
            >= (select max(inserted_date_time) from {{ this }})
        {% endif %}
)

,agg_events AS (
  SELECT
    user_id,
    max(external_user_ref) as external_user_ref, --had to convert these to max as some events missing channel / ext_user_ref
    max(channel) as channel,
    max(brand) as brand,
    max(case event_type when 'CREATED' then event_date_time end) as created_time,
    max(case event_type when 'DELETED' then event_date_time end) as deleted_time,
    count(case event_type when 'DELETED' then event_date_time end) = 0 as is_active,
    max(inserted_date_time) as inserted_date_time
  FROM
    user_events
    group by 
    user_id
)
,add_wallet_refresh AS (
  SELECT
    user_id,
    max(event_date_time) last_active
    from user_refresh
    group by user_id
)

,combine_together as (
    SELECT
    e.user_id,
    external_user_ref,
    channel,
    brand,
    created_time,
    deleted_time,
    last_active,
    is_active,
    inserted_date_time,
    sysdate() as updated_date_time
FROM
  agg_events e
  left join add_wallet_refresh a on a.user_id = e.user_id
)

select * from combine_together
