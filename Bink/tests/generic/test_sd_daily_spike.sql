/*
Generic test to ensure the number of events in the past day isn't outside a given multiple of the standard deviation beyond the median

Created By:     SP
Created Date:   2022/07/12
*/

{% test sd_daily_spike(model, column_name, val, datetime_col, unique_id_col, max_sd) %}
    {{ config(tags = ['business']) }}
    WITH count_new_vals AS (
        SELECT COUNT(*) c
        FROM {{ model }}
        WHERE {{ column_name }} = {{ "'" + val + "'"}}
        AND TO_DATE({{ datetime_col }}) = dateadd(day, -1, current_date())
    )
    , past_days AS (
        SELECT TO_DATE({{ datetime_col }}) AS date_part
        ,COUNT({{ unique_id_col }}) c
        FROM {{ model }}
        GROUP BY date_part
        HAVING DATEDIFF(day, date_part, CURRENT_DATE()) < 60
    )

    , ranges AS (
        SELECT
            STDDEV(c) AS sdev
            ,MEDIAN(c) AS med
            ,med + {{max_sd}} * sdev AS med_plus
            ,GREATEST(med - {{max_sd}} * sdev, 0) AS med_minus
        FROM past_days
    )

    ,is_beyond_range AS (
        SELECT
            CASE WHEN (SELECT med_plus FROM ranges) < c THEN true ELSE false END AS is_greater
            ,CASE WHEN (SELECT med_minus FROM ranges) > c THEN true ELSE false END AS is_less
        FROM count_new_vals
    )

    ,is_fail AS (
        SELECT * FROM is_beyond_range
        EXCEPT
        SELECT false AS is_greater, false AS is_less
    )

    SELECT * FROM is_fail

{% endtest %}