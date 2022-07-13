/*
Test to ensure no create events are followed by another create event

Created By:     SP
Created Date:   2022/07/12
*/

{% test consecutive_events(model, column_name, created_val, datetime_col, group_cols) %}

    {{ config(tags = ['business']) }}

    WITH all_events AS (
        SELECT
            {%- for item in group_cols-%}
            {%- if not loop.first %} , {% endif %} {{ item }} {%- endfor -%}
            ,{{column_name}}
            ,LEAD({{column_name}}) OVER
                (PARTITION BY {%- for item in group_cols-%} {%- if not loop.first %} , {% endif %} {{ item + ' '}}  {%- endfor -%}
                
                ORDER BY {{datetime_col}})
                AS NEXT_EVENT
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