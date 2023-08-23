with
    source as (select * from {{ ref("fact_pll_link_status_change_secure") }}),
    renamed as (
        select
            event_id,
            event_date_time,
            -- loyalty_card_id,
            loyalty_plan_id,
            loyalty_plan_company,
            loyalty_plan_name,
            -- payment_account_id,
            from_status_id,
            -- from_status,
            to_status_id,
            to_status,
            channel,
            -- brand,
            origin,
            user_id,
            -- external_user_ref,
            -- is_most_recent,
            -- inserted_date_time,
            updated_date_time
        from source
    )

select *
from renamed
