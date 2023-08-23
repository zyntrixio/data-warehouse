with
viator_ref_file as (
    select * from {{ source("RETAILER_LOOKUP", "VIATOR_REF_FILE") }}
),

ref_select as (
    select
        dashboard,
        category,
        metric_ref,
        metric_name
    from viator_ref_file
)

select *
from ref_select
