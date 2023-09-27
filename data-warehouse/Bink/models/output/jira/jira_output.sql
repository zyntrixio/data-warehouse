/*
Created by:         Christopher Mitchell
Created date:       2023-09-27
Last modified by:
Last modified date:

Description:
    output layer for Jira metrics 
Parameters:
    source_object       - jira_trans
*/

with stage as (
    select *
    from {{ ref('jira_trans') }}
)

select
    team,
    name,
    start_date,
    end_date,
    story_points_in_sprint_goal,
    story_points_carried_over,
    defects_in_sprint
from stage
