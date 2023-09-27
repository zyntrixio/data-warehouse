/*
Created by:         Christopher Mitchell
Created date:       2023-09-27
Last modified by:
Last modified date:

Description:
    output layer for Jira Wallet BI
Parameters:
    source_object       - jira_trans
*/

with stage as (
    select *
    from {{ ref('jira_trans') }}
    where team = 'Wallet'
)

select
    team,
    name,
    story_points_in_sprint_goal,
    story_points_carried_over,
    defects_in_sprint
from stage
