with
ref_file as (select * from {{ source("RETAILER_LOOKUP", "STONEGATE_FORECAST") }}
),
