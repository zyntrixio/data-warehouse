/*
Generic Test to ensure all delete events have a corresponding create event

Created By:     SP
Created Date:   2022/07/12
*/


{% test more_created_deleted(model, column_name, created_val, deleted_val, datetime_col, group_col) %}
    {{ config(tags = ['business']) }}

    WITH vals AS (
        SELECT 
            CASE WHEN {{column_name}} = {{ "'" + created_val + "'"}}
                THEN 1
                WHEN {{column_name}} = {{ "'" + deleted_val + "'"}}
                THEN -1
                ELSE 0
                END AS event_val
            ,{{group_col}}
            ,{{datetime_col}}
        FROM {{model}}
        WHERE {{group_col}} IS NOT null
    )

    ,sum_event_vals AS (
        SELECT
            {{group_col}} ,
            SUM(event_val) s,
            MAX({{datetime_col}}) max_time
        FROM
            vals
        GROUP BY
            {{group_col}}
        HAVING
            s < 0
            AND TIMEDIFF(hour, max_time, sysdate()) < 24
    )


    SELECT *
    FROM sum_event_vals

{% endtest %}