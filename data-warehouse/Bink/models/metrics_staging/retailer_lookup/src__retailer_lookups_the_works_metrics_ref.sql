WITH ref_file AS (
    SELECT *
    FROM {{source('RETAILER_LOOKUP','THE_WORKS_REF_FILE')}}
)

,ref_select as (
    SELECT 
        DASHBOARD
        ,CATEGORY
        ,METRIC_REF
        ,METRIC_NAME
    FROM ref_file )

select * from ref_select
