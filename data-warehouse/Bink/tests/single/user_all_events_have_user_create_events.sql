/*
Test to ensure all event tables with users have a corresponding create user event

Created By:     SP
Created Date:   2022/07/12
*/

-- depends_on: {{ ref('fact_user') }}
{{ config(
        tags=['business']
        ,severity = 'warn'
        ,meta={"description": "Test to ensure all event tables with users have a corresponding create user event 
                    - this is redundant for now as users created before events(could this be for lloyds only).", 
            "test_type": "Business"},
) }}

WITH all_users_from_events as (
    {% set get_tables  %}
    SELECT TABLE_NAME
    FROM PROD.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{{target.schema}}'
    AND TABLE_NAME LIKE 'FACT%'
    AND TABLE_NAME NOT LIKE 'FACT_USER%'
    {% endset %}

    {# Get column list #}
    {% set results = run_query(get_tables) %}

    {% if execute %}
    {# Return the first column as a list #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}


    {%- for item in results_list
    -%}
        {%- if item not in  ('FACT_VOUCHER', 'FACT_PLL_LINK_STATUS_CHANGE') %}
            {%- if not loop.first %} UNION {% endif %}
            SELECT USER_ID FROM PROD.{{target.schema}}.{{ item }}
            WHERE EVENT_DATE_TIME < (
                SELECT MAX(EVENT_DATE_TIME)
                FROM {{ref('fact_user')}}
                )
            AND EVENT_DATE_TIME > dateadd(day, -1, (
                SELECT MAX(EVENT_DATE_TIME)
                FROM {{ref('fact_user')}}
                )
                )
        {% endif %}
    {%- endfor -%}
)

, minus_create_users AS (
    SELECT USER_ID FROM all_users_from_events
    MINUS
    SELECT DISTINCT USER_ID
    FROM {{ref('fact_user')}}
)

SELECT *
FROM minus_create_users
