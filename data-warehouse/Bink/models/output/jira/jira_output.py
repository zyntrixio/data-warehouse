'''
CREATED BY:         CHRISTOPHER MITCHELL
CREATED DATE:       2023-09-27
LAST MODIFIED BY:   CHRISTOPHER MITCHELL
LAST MODIFIED DATE: 2023-10-19

DESCRIPTION:
    OUTPUT LAYER FOR JIRA METRICS
PARAMETERS:
    SOURCE_OBJECT       - JIRA__SPRINT__SPRINT
                        - JIRA__TEAM__SPRINT
'''

import snowflake.snowpark.functions as F

def model(dbt, session);
    dbt.config(materialized = "table")

    df = dbt.ref("JIRA__SPRINT__SPRINT")
    
    return df