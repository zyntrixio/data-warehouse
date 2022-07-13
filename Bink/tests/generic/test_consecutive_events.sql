/*
Test to ensure no create events are followed by another create event

Created By:     SP
Created Date:   2022/07/12
*/

{% test consecutive_events(model, column_name, created_val, datetime_col, group_col) %}

    {{ config(tags = ['business']) }}

    WITH all_events AS (
        SELECT
            {{group_col}}
            ,{{column_name}}
            ,LEAD({{column_name}}) OVER (PARTITION BY {{group_col}} ORDER BY {{datetime_col}}) AS NEXT_EVENT
        FROM {{model}}
        WHERE {{column_name}} = {{ "'" + created_val + "'"}}
    )

    ,consecutive_creates AS (
        SELECT *
        FROM all_events
        WHERE NEXT_EVENT = {{ "'" + created_val + "'"}}
    )

    SELECT *
    FROM consecutive_creates

{% endtest %}