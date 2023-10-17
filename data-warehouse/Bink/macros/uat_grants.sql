{# 
Created by:         Anand Bhakta
Created date:       2023-10-17
Last modified by:   
Last modified date: 

Description:
    This macro scans all schemas in database raw and applies grants to share prod-raw with uat

Parameters:

Returns:
        Returns no results but executes queries in line
#}

{% macro uat_grants(env) %}
{# DO I NEED TO DO THE INITAL SHARE GRANT OR IS THAT PERM#}

    {% set share_name %}
        {% if env == 'dev'%}
        "DEV_RAW"
        {% elif env == 'prod' %}
        "PROD-RAW"
        {% endif %}
    {% endset %}

   {% set get_schemas_query %}
        select schema_name from raw.information_schema.schemata;
    {% endset %}

    {%- set schemas = run_query(get_schemas_query) -%}

    {% if execute %}
    {# Return the first column #}
    {% set schemas_list = schemas.columns[0].values() %}
    {% else %}
    {% set schemas_list = [] %}
    {% endif %}


    {% for schema in schemas_list | unique -%}

        {% if schema != 'INFORMATION_SCHEMA'%}

        {% set grant_query_1%}
        GRANT USAGE ON SCHEMA RAW.{{schema}} TO SHARE {{share_name}}
        {% endset %}

        {% set grant_query_2%}
        GRANT SELECT ON ALL TABLES IN SCHEMA RAW.{{schema}} TO SHARE {{share_name}}
        {% endset %}

        {% do run_query(grant_query_1) %}
        {% do run_query(grant_query_2) %}

        {% endif %}

    {% endfor %}

{% endmacro %}
