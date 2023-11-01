/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-09-27
LAST MODIFIED BY:   CHRISTOPHER MITCHELL
LAST MODIFIED DATE: 2023-10-19

DESCRIPTION:
    OUTPUT LAYER FOR JIRA METRICS
PARAMETERS:
    SOURCE_OBJECT       - JIRA__SPRINT__SPRINT
                        - JIRA__TEAM__SPRINT
*/

with stage_sprint as (
    select
        *,
        'sprint' as category
    from {{ ref('jira__sprint__sprint') }}
),

stage_team as (
    select
        *,
        'team' as category
    from {{ ref('jira__team__sprint') }}
),

union_all as (
    select
        category,
        sprint_name,
        total_defects_in_sprint,
        total_story_points_in_sprint_goal,
        total_story_points_carried_over,
        total_tickets_accepted_in_sprints,
        sprint_quality,
        sprint_velocity,
        null as team,
        null as team_overall_sprint_quality,
        null as team_velocity
    from stage_sprint
    union all
    select
        category,
        null as sprint_name,
        null as total_defects_in_sprint,
        null as total_story_points_in_sprint_goal,
        null as total_story_points_carried_over,
        null as total_tickets_accepted_in_sprints,
        null as sprint_quality,
        null as sprint_velocity,
        team,
        team_overall_sprint_quality,
        team_velocity
    from stage_team
)

select *
from union_all
