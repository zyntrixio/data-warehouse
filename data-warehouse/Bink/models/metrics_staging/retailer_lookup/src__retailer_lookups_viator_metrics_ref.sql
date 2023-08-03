WITH viator_ref_file AS (
    SELECT *
    FROM {{source('RETAILER_LOOKUP','VIATOR_REF_FILE')}}
)

,ref_select as (
    SELECT 
        DASHBOARD
        ,CATEGORY
        ,METRIC_REF
        ,METRIC_NAME
    FROM viator_ref_file )

select * from ref_select
