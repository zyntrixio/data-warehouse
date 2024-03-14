with source as (
      select * from {{ source('RETAILER_LOOKUP', 'TGI_FRIDAYS_REF_FILE') }}
),
renamed as (
    select
        dashboard,
        category,
        metric_ref,
        metric_name
    from source
)
select * from renamed
  