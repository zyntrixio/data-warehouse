/*
Created by:         Anand Bhakta
Created date:       2023-06-07
Last modified by:   
Last modified date: 

Description:
    Datasource to show freshness of data in the metrics layer
Parameters:
    source_object       - src__fact_lc
*/
select max(inserted_date_time) as data_freshness from {{ ref("stg_metrics__fact_lc") }}
