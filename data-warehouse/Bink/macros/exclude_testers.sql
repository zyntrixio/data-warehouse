{# 
Created by:         Anand Bhakta
Created date:       2023-09-20
Last modified by:   
Last modified date: 

Description:
    This macro takes in a table or source table reference and then returns the columns as comparisons based on date 
    dynamically. This can also seperate out metrics from categorical inputs.

Parameters:
    node     - takes in table reference
    categorical       - takes list of string inputs which are columns from base table not to be converted to growth but included in select
    exclusion       - takes list of string inputs referencing columns in base table to exclude from query
    partition      - takes list of string inputs referencing columns in base table to exclude from query
    order      - takes a string to order data by

Returns:
        Returns a select statement to generate model
    
    
#}

{% macro exclude_testers(cols) %}

{%- for col in cols -%}
{{col}} not in (select {{col}} from {{ref('src__lookup_testers')}})
{% if not loop.last %}and{% endif %}
{% endfor %}

{% endmacro %}
