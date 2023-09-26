with stage as (
    select *
    from {{ ref('stg_metrics__jira_retailer') }}
)

select * from stage
