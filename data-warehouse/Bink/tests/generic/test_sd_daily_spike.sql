/*
Generic test to ensure the number of events in the past day isn't outside a given multiple of the standard deviation beyond the median

Created By:     SP
Created Date:   2022/07/12
*/
{% test sd_daily_spike(model, column_name, vals, datetime_col, unique_id_col, max_sd) %}
{{
    config(
        tags=["business"],
        meta={
            "description": "Generic test to ensure the number of events in the past day isn't outside a given multiple of the standard deviation beyond the median.",
            "test_type": "Business",
        },
    )
}}
with
    count_new_vals as (
        select count(*) c
        from {{ model }}
        where
            {{ column_name }} in (
                {%- for item in vals -%}
                {%- if not loop.first %}, {% endif %} {{ "'" + item + "' " }}
                {%- endfor -%}
            )
            and to_date({{ datetime_col }}) = dateadd(day, -1, current_date())
    ),
    past_days as (
        select to_date({{ datetime_col }}) as date_part, count({{ unique_id_col }}) c
        from {{ model }}
        group by date_part
        having datediff(day, date_part, current_date()) < 60
    ),
    ranges as (
        select
            stddev(c) as sdev,
            median(c) as med,
            med + {{ max_sd }} * sdev as med_plus,
            greatest(med - {{ max_sd }} * sdev, 0) as med_minus
        from past_days
    ),
    is_beyond_range as (
        select
            case
                when (select med_plus from ranges) < c then true else false
            end as is_greater,
            case
                when (select med_minus from ranges) > c then true else false
            end as is_less
        from count_new_vals
    ),
    is_fail as (
        select *
        from is_beyond_range
        except
        select false as is_greater, false as is_less
    )

select *
from is_fail

{% endtest %}
