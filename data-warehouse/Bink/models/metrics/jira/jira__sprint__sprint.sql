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

WITH stage AS (
    SELECT *
    FROM metrics_staging.transformation.jira_trans
),

rename AS (
    SELECT
        team,
        name AS sprint_name,
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
    FROM stage
),

sprint_metrics_stage AS (
    SELECT
        team,
        sprint_name,
        SUM(defect_count) AS total_defects_in_sprint,
        SUM(story_points_in_sprint_goal) AS total_story_points_in_sprint_goal,
        SUM(story_points_carried_over) AS total_story_points_carried_over,
        SUM(tickets_accepted_in_sprint) AS total_tickets_accepted_in_sprints
    FROM rename
    GROUP BY team, sprint_name
),

sprint_metrics_calc AS (
    SELECT
        team,
        sprint_name,
        total_defects_in_sprint,
        total_story_points_in_sprint_goal,
        total_story_points_carried_over,
        total_tickets_accepted_in_sprints,
        IFF(
            total_story_points_in_sprint_goal = 0, 0,
            100
            - (
                100
                * (total_defects_in_sprint / total_story_points_in_sprint_goal)
            )
        ) AS sprint_quality,
        IFF(
            total_story_points_in_sprint_goal = 0,
            0,
            total_tickets_accepted_in_sprints
            / total_story_points_in_sprint_goal
        ) AS sprint_velocity
    FROM sprint_metrics_stage
)

SELECT *
FROM sprint_metrics_calc
