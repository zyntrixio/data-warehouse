with stage as (
    select *
    from {{ ref('stg_metrics__jira_wallet') }}
)

select * from stage
