/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   Anand Bhakta
Last modified date: 2023-12-11

Description:
	The output Dimension table for loyalty cards

Parameters:
    ref_object      - stg_hermes__SCHEME_SCHEMEACCOUNT
    ref_object      - stg_hermes__SCHEME_SCHEME
    ref_object      - stg_hermes__SCHEME_CATEGORY
*/

{{
    config(
        alias="dim_loyalty_card",
        materialized="incremental",
        unique_key="unique_key",
    )
}}

with lc_events as (select * from {{ ref("fact_loyalty_card_secure") }}
        {% if is_incremental() %}
            where loyalty_card_id in (
                select distinct loyalty_card_id from {{ ref("fact_loyalty_card_secure") }}
                where inserted_date_time 
            >= (select max(inserted_date_time) from {{ this }}))
        {% endif %}
)

,ranked_events AS (
  SELECT
    loyalty_card_id,
    user_id,
    external_user_ref,
    loyalty_plan_name,
    loyalty_plan_company,
    channel,
    brand,
    event_type,
    auth_type,
    event_date_time,
    ROW_NUMBER() OVER (PARTITION BY loyalty_card_id, user_id ORDER BY event_date_time) AS event_rank_oldest,
    ROW_NUMBER() OVER (PARTITION BY loyalty_card_id, user_id ORDER BY event_date_time DESC) AS event_rank_newest,
    MIN(CASE WHEN event_type = 'SUCCESS' THEN event_date_time END) 
      OVER (PARTITION BY loyalty_card_id, user_id) AS first_successful_event_time,
    COUNT(DISTINCT user_id) OVER (PARTITION BY loyalty_card_id) AS user_count,
    inserted_date_time
  FROM
    lc_events
)
,add_multi_channel AS (
  SELECT
    loyalty_card_id,
    user_id,
    external_user_ref,
    loyalty_plan_name,
    loyalty_plan_company,
    channel,
    brand,
    event_type,
    auth_type,
    event_date_time,
    event_rank_oldest,
    event_rank_newest,
    first_successful_event_time,
    count(distinct case when event_rank_newest = 1 and event_type = 'SUCCESS' then user_id end) 
        over (partition by loyalty_card_id) AS live_user_count,
    inserted_date_time
  FROM
    ranked_events
        
)

,grouped_events as (SELECT
    loyalty_card_id||'-'||user_id as unique_key,
    loyalty_card_id,
    user_id,
    MAX(external_user_ref) AS external_user_ref,
    MAX(loyalty_plan_name) AS loyalty_plan_name,
    MAX(loyalty_plan_company) AS loyalty_plan_company,
    MAX(channel) AS channel,
    MAX(brand) AS brand,
    MAX(CASE WHEN event_rank_oldest = 1 THEN event_date_time END) AS first_event_time,
    MAX(CASE WHEN event_rank_oldest = 1 THEN auth_type END) AS first_auth_type,
    max(first_successful_event_time) AS first_successful_event_time,
    MAX(CASE WHEN event_date_time = first_successful_event_time THEN auth_type END) AS first_successful_auth_type,
    MAX(CASE WHEN event_rank_newest = 1 THEN event_date_time END) AS most_recent_event_time,
    MAX(CASE WHEN event_rank_newest = 1 THEN event_type END) AS most_recent_event_type,
    max(CASE WHEN live_user_count > 1 THEN TRUE ELSE FALSE END) AS multi_user_card,
    MAX(CASE WHEN event_rank_newest = 1 THEN event_type END) = 'SUCCESS' AS is_active,
    max(inserted_date_time) as  inserted_date_time,
    sysdate() as updated_date_time
FROM
  add_multi_channel
  group by   
    unique_key,
    loyalty_card_id,
    user_id
    )

  select * from grouped_events
