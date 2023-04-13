{% test is_year(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 2000 OR  {{ column_name }} > 2050

{% endtest %}