with events_src as (
    SELECT EVENT_ID
    FROM {{ ref('stg_hermes__EVENTS')}}
)

 ,fact_tables as (
    {% set get_tables  %}
    SELECT TABLE_NAME
    FROM {{target.database}}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{{target.schema}}'
    AND TABLE_NAME LIKE 'FACT%'
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
    {%- if not loop.first %} UNION ALL {% endif %}
    SELECT event_id FROM {{target.database}}.{{target.schema}}.{{ item }}
    {%- endfor -%}
)

,events_minus_facts as (
    SELECT EVENT_ID FROM events_src
    EXCEPT 
    SELECT EVENT_ID FROM fact_tables
)

,facts_minus_events as (
    SELECT EVENT_ID FROM fact_tables
    EXCEPT
    SELECT EVENT_ID FROM events_src
)

,sum_except_all as (
    SELECT * FROM events_minus_facts
    UNION ALL
    SELECT * FROM facts_minus_events
)

SELECT * FROM sum_except_all