/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-10-17
LAST MODIFIED BY:
LAST MODIFIED DATE:

DESCRIPTION:
    JIRA METRICS FOR SPRINTS BROKEN DOWN MONTHLY BY SPRINT
PARAMETERS:
    SOURCE_OBJECT       - JIRA_TRANS
TEAM METRICS:
    DEFECTS IN SPRINT -> TOTAL DEFECTS IN SPRINT
    SPRINT QUALITY -> 100 - (100 * (TOTAL NUMBER OF DEFECTS FROM EACH SPRINT/TOTAL STORY POINTS COMPLETED FROM EACH SPRINT))

*/

with stage as (
    select *
    from metrics_staging.transformation.jira_trans
),

rename as (
    select
        team,
        iff(team is not null, concat(team, ' - ', name), name) as sprint_name,
        -- goal,
        start_date,
        end_date,
        ticket_total,
        ticket_carry_over_count,
        user_story_count,
        investigation_count,
        bug_count,
        defect_count,
        tickets_accepted_in_sprint,
        story_points_in_sprint_goal,
        story_points_carried_over,
        -- api_banking_release,
        -- api_consumer_release,
        defects_in_sprint
    -- tech_tickets,
    -- security_tickets,
    -- devops_tickets,
    -- misc_technical_tickets,
    -- product_tickets,
    -- bau_product,
    -- project,
    from stage
),

sprint_metrics_stage as (
    select
        sprint_name,
        sum(defect_count) as total_defects_in_sprint,
        sum(story_points_in_sprint_goal) as total_story_points_in_sprint_goal,
        sum(story_points_carried_over) as total_story_points_carried_over,
        sum(tickets_accepted_in_sprint) as total_tickets_accepted_in_sprints
    from rename
    group by sprint_name
),

sprint_metrics_calc as (
    select
        sprint_name,
        total_defects_in_sprint,
        total_story_points_in_sprint_goal,
        total_story_points_carried_over,
        total_tickets_accepted_in_sprints,
        iff(
            total_story_points_in_sprint_goal = 0, 0,
            100
            - (100 * (total_defects_in_sprint / total_story_points_in_sprint_goal))
        ) as sprint_quality,
        iff(
            total_story_points_in_sprint_goal = 0,
            0,
            total_tickets_accepted_in_sprints
            / total_story_points_in_sprint_goal
        ) as sprint_velocity
    from sprint_metrics_stage
)

select *
from sprint_metrics_calc
