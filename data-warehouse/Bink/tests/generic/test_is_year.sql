{% test is_year(model, column_name) %}
select * from {{ model }} where {{ column_name }} < 2000 or {{ column_name }} > 2050

{% endtest %}
