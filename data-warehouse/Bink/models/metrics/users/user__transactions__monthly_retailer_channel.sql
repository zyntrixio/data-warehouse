/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-11-02
LAST MODIFIED BY:   
LAST MODIFIED DATE: 

DESCRIPTION:
    USER TRANSACTION METRICS MONTHLY BY RETAILER
PARAMETERS:
    SOURCE_OBJECT       - TXNS_TRANS
                        - STG_METRICS__DIM_DATE
*/
with
user_events as (select * from {{ ref("txns_trans") }}),

dim_date as (
    select distinct
        start_of_month,
        end_of_month
    from {{ ref("stg_metrics__dim_date") }}
    where date >= (select min(date) from user_events) and date <= current_date()
),

user_snap as (
    select
        d.start_of_month as date,
        u.channel,
        u.brand,
        u.loyalty_plan_company,
        u.loyalty_plan_name,
        coalesce(count(
            DISTINCT CASE WHEN STATUS = 'TXNS' THEN user_ref END
        ),0) as U201_ACTIVE_USERS_RETAILER_MONTHLY_CHANNEL__CDCOUNT_UID,
        coalesce(count(
            DISTINCT CASE WHEN STATUS IN ('TXNS', 'REFUND') THEN user_ref END
        ),0) as U202_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__CDCOUNT_UID
    from user_events u
    left join dim_date d on date(u.date) <= d.end_of_month
    group by d.start_of_month, u.channel, u.brand, u.loyalty_plan_company, u.loyalty_plan_name
),

user_period as (
    select
        d.start_of_month as date,
        u.channel,
        u.brand,
        u.loyalty_plan_company,
        u.loyalty_plan_name,
        coalesce(
            count(DISTINCT CASE WHEN STATUS = 'TXNS' THEN user_ref END), 0
        ) as U200_ACTIVE_USERS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID,
        coalesce(
            count(DISTINCT CASE WHEN STATUS IN ('TXNS', 'REFUND') THEN user_ref END), 0
        ) as U203_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID
    from user_events u
    left join dim_date d on d.start_of_month = date_trunc('month', u.date)
    group by d.start_of_month, u.channel, u.brand, u.loyalty_plan_company, u.loyalty_plan_name
),

combine_all as (
    select
        coalesce(s.date, p.date) as date,
        coalesce(s.channel, p.channel) as channel,
        coalesce(s.brand, p.brand) as brand,
        coalesce(
            s.loyalty_plan_company, p.loyalty_plan_company
        ) as loyalty_plan_company,
        coalesce(s.loyalty_plan_name, p.loyalty_plan_name) as loyalty_plan_name,
        coalesce(
            s.U201_ACTIVE_USERS_RETAILER_MONTHLY_CHANNEL__CDCOUNT_UID, 0
        ) as U201_ACTIVE_USERS_RETAILER_MONTHLY_CHANNEL__CDCOUNT_UID,
        coalesce(
            p.U200_ACTIVE_USERS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID, 0
        ) as U200_ACTIVE_USERS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID,
        coalesce(
            s.U202_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__CDCOUNT_UID, 0
        ) as U202_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__CDCOUNT_UID,
        coalesce(
            p.U203_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID, 0
        ) as U203_ACTIVE_USERS_INC_REFUNDS__RETAILER_MONTHLY_CHANNEL__DCOUNT_UID
    from user_snap s
    full outer join
        user_period p
        on
            s.date = p.date
            and s.loyalty_plan_company = p.loyalty_plan_company
            and s.channel = p.channel
            and s.brand = p.brand
)

select *
from combine_all
