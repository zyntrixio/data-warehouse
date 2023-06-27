/*
Created by:         Anand Bhakta
Created date:       2023-06-27
Last modified by:   
Last modified date: 

Description:
    Sankey funnel data for lloyds to be joined with the sankey model table
Parameters:
    source_object      - src__fact_lc_add
*/

with lc as (
    select
        *
    from
        {{ref('src__fact_lc')}}
    where
        EVENT_TYPE != 'REQUEST'
),

lc_group as (
    SELECT
        EXTERNAL_USER_REF,
        USER_ID,
        COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID) AS USER_REF,
        EVENT_DATE_TIME,
        BRAND,
        CHANNEL,
        CASE WHEN lc.AUTH_TYPE IN ('JOIN', 'REGISTER') THEN 'JOIN' ELSE 'LINK' END as AUTH_TYPE,
        EVENT_TYPE,
        LOYALTY_CARD_ID,
        LOYALTY_PLAN_NAME,
        LOYALTY_PLAN_COMPANY,
        SUM(
            CASE
                WHEN EVENT_TYPE = 'SUCCESS' THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY COALESCE(NULLIF(EXTERNAL_USER_REF,''), USER_ID),
            LOYALTY_PLAN_NAME
            ORDER BY
                EVENT_DATE_TIME ASC
        ) as LC_GROUP
    from
        lc
),

lc_group2 as (
    SELECT
        USER_REF,
        EVENT_DATE_TIME,
        BRAND,
        CHANNEL,
        AUTH_TYPE,
        EVENT_TYPE,
        LOYALTY_CARD_ID,
        LOYALTY_PLAN_NAME,
        LOYALTY_PLAN_COMPANY,
        CASE
            WHEN EVENT_TYPE = 'SUCCESS' THEN LC_GROUP -1
            ELSE LC_GROUP
        END as LC_GROUP
    from
        lc_group
),

modify_fields as (
    SELECT
        USER_REF,
        EVENT_DATE_TIME,
        BRAND,
        CHANNEL,
        AUTH_TYPE,
        first_value(AUTH_TYPE) over (partition by USER_REF,
            LOYALTY_PLAN_NAME,
            lc_group
            order by EVENT_DATE_TIME asc) as ORG_AUTH_TYPE,
        first_value(EVENT_DATE_TIME) over (partition by USER_REF,
            LOYALTY_PLAN_NAME,
            lc_group
            order by EVENT_DATE_TIME asc) as START_TIME,
        lag(auth_type) over (partition by USER_REF,
            LOYALTY_PLAN_NAME,
            lc_group
            order by EVENT_DATE_TIME asc) as PREV_AUTH_TYPE,
        row_number() over (partition by USER_REF,
            LOYALTY_PLAN_NAME,
            lc_group
            order by EVENT_DATE_TIME asc) as ATTEMPT,
        last_value(concat(AUTH_TYPE, ' ', EVENT_TYPE)) over (partition by USER_REF,
            LOYALTY_PLAN_NAME,
            lc_group
            order by EVENT_DATE_TIME asc) as LAST_VAL,
        concat(AUTH_TYPE, ' ', EVENT_TYPE) as EVENT_TYPE,
        LOYALTY_CARD_ID,
        LOYALTY_PLAN_NAME,
        LOYALTY_PLAN_COMPANY,
        LC_GROUP
        from lc_group2
),

select_fields as (
    SELECT
        LOYALTY_PLAN_NAME,
        LOYALTY_PLAN_COMPANY,
        BRAND,
        CHANNEL,
        concat(USER_REF, ' ', LOYALTY_PLAN_NAME, ' ', lc_group) as ID,
        ORG_AUTH_TYPE,
        1 as SIZE,
        'link' as LINK,
        START_TIME,
        ATTEMPT,
        CASE WHEN ATTEMPT = 5 then LAST_VAL
        else EVENT_TYPE end as EVENT_TYPE
    from
        modify_fields
        
),


pivot as (select * from select_fields
    PIVOT(min(EVENT_TYPE) for ATTEMPT in (1,2,3,4,5)))

select * from pivot
