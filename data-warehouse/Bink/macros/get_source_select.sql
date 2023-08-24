{# 
Created by:         Aidan Summerville
Created date:       2022-03-30
Last modified by:   
Last modified date: 

Description:
    This macro takes in a table or source table reference and then returns the column names 
    in list format to be used to dynamcally build a select statement with.

Parameters:
    base_table     - takes in either a source table reference or model reference

Returns:
        Returns a list of columns that exist in that table in alpahbetical order
Usage:
    This was built for use in dynamically returning a list of all columns in a table or view
    It can then be used to dynamcically build a base select satement savingtime across large tables
    Espicllay useful for staing tables
    
    
#}

{% macro get_source_select(base_table) %}

{# returns a table with a singular column taht  #}

{% set dbaseselect  %}

select split_part('{{base_table}}','.',1) as col


{% endset %}

{% set dbaselist = run_query(dbaseselect) %}

{% if execute %}
{# Return the first column #}
{% set dbase = dbaselist.columns[0][0] %}
{% endif %}

{% set exist  %}





select column_NAME

from {{ dbase|replace("'","") }}.INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = split_part('{{base_table}}','.',3)
and TABLE_SCHEMA = split_part('{{base_table}}','.',2)
-- order by column_NAME

{% endset %}

{% set results = run_query(exist) %}

{% if execute %}
{# Return the first column as  a list#}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
