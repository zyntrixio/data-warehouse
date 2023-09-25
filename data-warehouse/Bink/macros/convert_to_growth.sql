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

{% macro convert_to_growth(node, categorical, exclusion, partition, order) %}

with metrics as (select * from {{ ref(node) }})

,growth as (
    select
        {%- for col in adapter.get_columns_in_relation(ref(node)) -%}
        {%- if col.column in categorical %}
        {{ col.column}},
        {%- else %}
        div0({{ col.column}}, lag({{ col.column}}) over (partition by {% for col in partition %} {{col}} {%- if not loop.last %} , {% endif -%} {% endfor %} order by {{order}})) - 1 as {{ col.column}}__GROWTH
        {%- if not loop.last %} , {% endif -%}
        {%- endif %}
        {%- endfor %}
    from
        metrics
)

select * from growth

{% endmacro %}
