/*
Created by:         Anand Bhakta
Created date:       2023-07-04
Last modified by:
Last modified date:

Description:
    Fact table for loyalty card and payment card pll link status
	Incremental strategy: loads all newly inserted records, transforms, then loads
	all events which require updating, finally calculating is_most_recent
	flag, and merging based on the event id

Parameters:
    ref_object      - transformed_hermes_events
*/
{{
    config(
        alias="fact_pll_link_status_change",
        materialized="incremental",
        unique_key="EVENT_ID",
        merge_update_columns=["IS_MOST_RECENT", "UPDATED_DATE_TIME"],
    )
}}


with
pll as (
    select *
    from {{ ref("fact_pll_link_status_change_secure") }}
    {% if is_incremental() %}
        where
            updated_date_time >= (select max(updated_date_time) from {{ this }})
    {% endif %}
),

pll_select as (
    select
        event_id,
        event_date_time,
        loyalty_card_id,
        loyalty_plan_id,
        loyalty_plan_company,
        loyalty_plan_name,
        payment_account_id,
        from_status_id,
        from_status,
        to_status_id,
        to_status,
        channel,
        brand,
        origin,
        user_id,
        -- external_user_ref,
        case
            when
                (
                    event_date_time = max(event_date_time) over (
                        partition by loyalty_card_id, payment_account_id
                    )
                )
                then true
            else false
        end as is_most_recent,
        inserted_date_time,
        sysdate() as updated_date_time
    from pll
)

select *
from pll_select
