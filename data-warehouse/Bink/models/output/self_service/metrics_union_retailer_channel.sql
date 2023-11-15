/*
Created by:         Anand Bhakta
Created date:       2023-10-11
Last modified by:
Last modified date:

Description:
    Datasource to produce full list of metrics for tableau self-serve
Parameters:
    source_object       - lc__links_joins__monthly_retailer
                        - trans__trans__monthly_retailer
                        - trans__avg__monthly_retailer
                        - user__transactions__monthly_retailer

 depends_on:    {{ ref('lc__pll__monthly_retailer') }}
                {{ ref('lc__pll__monthly_retailer_channel')}}
                {{ ref('lc__links_joins__daily_retailer_channel') }}
                {{ ref('lc__links_joins__monthly_retailer') }}
                {{ ref('lc__links_joins__daily_retailer') }}
                {{ ref('lc__links_joins__monthly_retailer_channel') }}
                {{ ref('trans__trans__monthly_retailer') }}
                {{ ref('trans__avg__monthly_retailer') }}
                {{ ref('trans__trans__daily_retailer') }}
                {{ ref('trans__trans__daily_retailer_channel') }}
                {{ ref('voucher__counts__monthly_retailer') }}
                {{ ref('voucher__counts__daily_channel_brand_retailer') }}
                {{ ref('voucher__times__voucher_level_channel_brand') }}
                {{ ref('user__transactions__monthly_channel_brand_retailer') }}
                {{ ref('user__registrations__daily_channel_brand') }}
                {{ ref('user__transactions__monthly_retailer') }}
                {{ ref('user__loyalty_card__daily_channel_brand') }}
                {{ ref('trans__avg__monthly_retailer_channel')}}
                {{ ref('trans__trans__monthly_retailer_channel')}}
                {{ ref('user__transactions__monthly_retailer_channel')}}
*/


{% for model in graph.nodes.values() 
    if  model.resource_type == "model" and
        model.database == "METRICS" and
        "growth" not in model.name and
        "USER_LEVEL" not in model.name and
        "level" not in model.name and
        'retailer' in model.name and
        'channel' in model.name and
        'forecast' not in model.name
    %}
                {{
            dbt_utils.unpivot(
                relation=ref(model.name),
                cast_to="number(38,2)",
                exclude=[
                    "date",
                    "loyalty_plan_company",
                    "loyalty_plan_name",
                    "channel",
                    "brand"
                ],
                field_name="metric",
                value_name="value",
            )
        }}
    {%-if not loop.last-%}UNION{%endif%}

{% endfor %}
