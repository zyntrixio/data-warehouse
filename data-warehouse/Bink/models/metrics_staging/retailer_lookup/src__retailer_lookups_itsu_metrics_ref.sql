with
ref_file as (select * from {{ source("RETAILER_LOOKUP", "ITSU_REF_FILE") }}
),

ref_select as (
    select
        dashboard,
        category,
        metric_ref,
        metric_name
    from ref_file
)

select *
from ref_select
