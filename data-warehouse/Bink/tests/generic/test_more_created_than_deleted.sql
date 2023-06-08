/*
Generic Test to ensure sum of delete and create events is not less than 0 or greater than 1

Created By:     SP
Created Date:   2022/07/12
Last modified by:   AB
Last modified date: 2022/10/26
*/


{% test more_created_deleted(model, column_name, created_val, deleted_val, datetime_col, group_col, filter_date) %}
    {{ config(
        tags=['business']
        ,meta={"description": "Generic Test to ensure sum of delete and create events is not less than 0 or greater than 1.", 
            "test_type": "Business"},
) }}

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
            ,{{column_name}}
            ,RANK() OVER ( PARTITION BY {{group_col}} ORDER BY {{datetime_col}} ASC) AS r
        FROM {{model}}
        WHERE {{group_col}} IS NOT null
        AND {{column_name}} IN ({{ "'" + created_val + "'"}}, {{ "'" + deleted_val + "'"}})
        AND {{datetime_col}} > {{"'" + filter_date + "'"}}
    )

    , create_first AS (
        SELECT 
            {{group_col}}
            ,{{column_name}}
        FROM
            vals
        WHERE
            r = 1
            AND {{column_name}} = {{ "'" + created_val + "'"}}
    )


    ,sum_event_vals AS (
        SELECT
            {{group_col}} ,
            SUM(event_val) s,
            MAX({{datetime_col}}) max_time
        FROM
            vals
        WHERE
            {{group_col}} IN (SELECT {{group_col}} FROM create_first)
        GROUP BY
            {{group_col}}
        HAVING
            s NOT IN (0,1)
            AND TIMEDIFF(hour, max_time, sysdate()) < 24
    )


    SELECT *
    FROM sum_event_vals

{% endtest %}
