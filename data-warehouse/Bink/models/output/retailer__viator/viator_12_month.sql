/*
Created by:         Anand Bhakta
Created date:       2023-08-03
Last modified by:    
Last modified date: 

Description:
    Datasource to produce tableau dashboard for Viator 12 month rolling datasource
Parameters:
    source_object       - viator_dashboard_view
*/

with unpivot as ({{ dbt_utils.unpivot(
  relation=ref('viator_dashboard_view'),
  cast_to='number(38,2)',
  exclude=['date', 'category', 'loyalty_plan_company', 'loyalty_plan_name'],
  field_name='metric',
  value_name='value'
) }})

,refs as (SELECT * FROM {{ref('src__retailer_lookups_viator_metrics_ref')}}
          WHERE DASHBOARD = '12_MONTH_ROLLING'
          )

,rename as (
  select 
    DATE
    ,u.CATEGORY
    ,u.LOYALTY_PLAN_COMPANY
    ,u.LOYALTY_PLAN_NAME
    ,u.METRIC
    ,r.METRIC_NAME
    ,u.VALUE
  FROM unpivot u
  LEFT JOIN refs r ON r.metric_ref = u.metric
  WHERE VALUE IS NOT NULL
)

select * from rename

    
