/*
Created by:         Christopher Mitchell
Created date:       2023-09-27
Last modified by:
Last modified date:

Description:
    Transformation for the Jira reporting soluiton, this does not include making metrics as this is just to combine the 3 teams into one big table for tableau use later in the data flow
Parameters:
    source_object       - stg_metrics__jira_data_product
                        - stg_metrics__jira_retailer
                        - stg_metrics__jira_wallet
*/

WITH data_stage AS (
    SELECT
        *,
        'Data' AS team
    FROM {{ ref('stg_metrics__jira_data_product') }}
),

ret_stage AS (
    SELECT
        *,
        'Retailer' AS team
    FROM {{ ref('stg_metrics__jira_retailer') }}
),

wal_stage AS (
    SELECT
        *,
        'Wallet' AS team
    FROM {{ ref('stg_metrics__jira_wallet') }}
),

combine AS (
    SELECT
        team,
        name,
        goal,
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
        NULL AS api_banking_release,
        NULL AS api_consumer_release,
        defects_in_sprint,
        tech_tickets,
        security_tickets,
        devops_tickets,
        misc_technical_tickets,
        product_tickets,
        bau_product,
        project
    FROM data_stage
    UNION ALL
    SELECT
        team,
        name,
        goal,
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
        NULL AS api_banking_release,
        NULL AS api_consumer_release,
        defects_in_sprint,
        tech_tickets,
        security_tickets,
        devops_tickets,
        misc_technical_tickets,
        product_tickets,
        bau_product,
        project
    FROM ret_stage
    UNION ALL
    SELECT
        team,
        name,
        goal,
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
        api_banking_release,
        api_consumer_release,
        defects_in_sprint,
        tech_tickets,
        security_tickets,
        devops_tickets,
        misc_technical_tickets,
        product_tickets,
        bau_product,
        project
    FROM wal_stage
)

SELECT * FROM combine
