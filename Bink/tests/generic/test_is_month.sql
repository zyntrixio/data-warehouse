{% test is_month(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 1 OR  {{ column_name }} > 12

{% endtest %}