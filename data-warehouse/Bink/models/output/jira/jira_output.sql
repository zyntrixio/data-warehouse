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

WITH stage_sprint AS (
    SELECT *, 'sprint' AS category
    FROM {{ ref('jira__sprint__sprint') }}),

     stage_team AS (
         SELECT *, 'team' AS category
         FROM {{ ref('jira__team__sprint') }}),

     union_all AS (
         SELECT category,
                sprint_name,
                total_defects_in_sprint,
                total_story_points_in_sprint_goal,
                total_story_points_carried_over,
                total_tickets_accepted_in_sprints,
                sprint_quality,
                sprint_velocity,
                NULL AS team,
                NULL AS team_overall_sprint_quality,
                NULL AS team_velocity
         FROM stage_sprint
         UNION ALL
         SELECT category,
                NULL AS sprint_name,
                NULL AS total_defects_in_sprint,
                NULL AS total_story_points_in_sprint_goal,
                NULL AS total_story_points_carried_over,
                NULL AS total_tickets_accepted_in_sprints,
                NULL AS sprint_quality,
                NULL AS sprint_velocity,
                team,
                team_overall_sprint_quality,
                team_velocity
         FROM stage_team)

SELECT *
FROM union_all