/*
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-09-27
LAST MODIFIED BY:   CHRISTOPHER MITCHELL
LAST MODIFIED DATE: 2023-10-23

DESCRIPTION:
    TRANSFORMATION FOR THE JIRA REPORTING SOLUITON, THIS DOES NOT INCLUDE MAKING METRICS AS THIS IS JUST TO COMBINE THE 3 TEAMS INTO ONE BIG TABLE FOR TABLEAU USE LATER IN THE DATA FLOW
PARAMETERS:
    SOURCE_OBJECT       - SRC__JIRA_DATA_PRODUCT
                        - SRC__JIRA_RETAILER
                        - SRC__JIRA_WALLET
*/

WITH data_stage AS (
    SELECT
        *,
        'Data' AS team
    FROM {{ ref('src__jira_data_product') }}
),

ret_stage AS (
    SELECT
        *,
        'Retailer' AS team
    FROM {{ ref('src__jira_retailer') }}
    WHERE start_date >= '2023-02-27'
),

wal_stage AS (
    SELECT
        *,
        'Wallet' AS team
    FROM {{ ref('src__jira_wallet') }}
    WHERE start_date >= '2023-02-27'
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
),

date_trans AS (
    SELECT
        team,
        name,
        goal,
        DATE(start_date) as start_date,
        DATE(end_date) as end_date,
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
    FROM combine
)

SELECT * FROM date_trans