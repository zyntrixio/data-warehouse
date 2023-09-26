with stage as (
    select *
    from {{ ref('stg_metrics__jira_data_product') }}
)

select * from stage
