/*
CREATED BY:         CHRISTOPHER MITCHELL
Last modified by:
Last modified date: 2024-03-12

DESCRIPTION:
    USER TRANSACTION METRICS MONTHLY BY RETAILER
PARAMETERS:
    SOURCE_OBJECT       - TXNS_TRANS
                        - STG_METRICS__DIM_DATE
*/
with user_events as (
    select *
    from
        {{ ref('txns_trans') }}
),

dim_date as (
    select *
    from
        {{ ref('stg_metrics__dim_date') }}
    where
        date >= (
            select min(date(date))
            from
                user_events
        )
        and date <= current_date()
),

user_period as (
    select
        d.date as date,
        u.channel,
        u.brand,
        u.loyalty_plan_company,
        u.loyalty_plan_name,
        coalesce(
            count(
                distinct case
                    when status = 'TXNS' then user_ref
                end
            ),
            0
        ) as u008_active_users__daily_channel_brand_retailer__dcount_uid,
        coalesce(
            count(
                distinct case
                    when status in ('TXNS', 'REFUND') then user_ref
                end
            ),
            0
        ) as u009_active_users_inc_refunds__daily_channel_brand_retailer__dcount_uid
    from
        user_events u
    left join dim_date d on d.date = date(u.date)
    group by
        d.date,
        u.brand,
        u.channel,
        u.loyalty_plan_company,
        u.loyalty_plan_name
)

select *
from
    user_period
