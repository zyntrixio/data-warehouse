/*
Created by:         Christopher Mitchell 
Created date:       2023-07-03
Last modified by:   
Last modified date: 

Description:
    todo
Parameters:
    source_object       - pll_status_trans
*/
WITH pll_events AS (
    SELECT *
    FROM {{ ref('pll_status_trans') }})

, dim_date AS (
    SELECT DISTINCT start_of_month, end_of_month
    FROM {{ ref('dim_date') }}
    WHERE date >= (
        SELECT MIN(from_date)
        FROM pll_events)
      AND date <= CURRENT_DATE())

,count_up_snap AS (
    SELECT d.start_of_month                                                                       AS date
         , u.loyalty_plan_name
         , u.loyalty_plan_company
        , COALESCE(COUNT(DISTINCT CASE WHEN ACTIVE_LINK THEN LOYALTY_CARD_ID END), 0)             AS pll_active_link_count
    FROM pll_events u
             LEFT JOIN dim_date d
                       ON d.end_of_month >= DATE(u.from_date)
                           AND d.end_of_month < COALESCE(DATE(u.to_date), '9999-12-31')
    GROUP BY d.start_of_month
           , u.loyalty_plan_name
           , u.loyalty_plan_company
    HAVING date IS NOT NULL)

,rename AS (
    SELECT
        DATE
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,PLL_ACTIVE_LINK_COUNT AS lc201__loyalty_card_active_pll__monthly_retailer__pit
    FROM
        count_up_snap
)

select * from rename
