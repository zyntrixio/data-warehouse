/*
Generic Test to ensure sum of delete and create events is not less than 0 or greater than 1

Created By:     SP
Created Date:   2022/07/12
Last modified by:   AB
Last modified date: 2022/10/26
*/
{% test more_created_deleted(
    model,
    column_name,
    created_val,
    deleted_val,
    datetime_col,
    group_col,
    filter_date
) %}
{{
    config(
        tags=["business"],
        meta={
            "description": "Generic Test to ensure sum of delete and create events is not less than 0 or greater than 1.",
            "test_type": "Business",
        },
    )
}}

with
    vals as (
        select
            case
                when {{ column_name }} = {{ "'" + created_val + "'" }}
                then 1
                when {{ column_name }} = {{ "'" + deleted_val + "'" }}
                then -1
                else 0
            end as event_val,
            {{ group_col }},
            {{ datetime_col }},
            {{ column_name }},
            rank() over (
                partition by {{ group_col }} order by {{ datetime_col }} asc
            ) as r
        from {{ model }}
        where
            {{ group_col }} is not null
            and {{ column_name }}
            in ({{ "'" + created_val + "'" }}, {{ "'" + deleted_val + "'" }})
            and {{ datetime_col }} > {{ "'" + filter_date + "'" }}
    ),
    create_first as (
        select {{ group_col }},{{ column_name }}
        from vals
        where r = 1 and {{ column_name }} = {{ "'" + created_val + "'" }}
    ),
    sum_event_vals as (
        select {{ group_col }}, sum(event_val) s, max({{ datetime_col }}) max_time
        from vals
        where {{ group_col }} in (select {{ group_col }} from create_first)
        group by {{ group_col }}
        having s not in (0, 1) and timediff(hour, max_time, sysdate()) < 24
    )

select *
from sum_event_vals

{% endtest %}
