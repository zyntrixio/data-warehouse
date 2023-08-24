/*
Created by:         Anand Bhakta
Created date:       2023-06-07
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-23

Description:
    Datasource to show freshness of data in the metrics layer
Parameters:
    source_object       - stg_metrics__fact_lc
*/
select max(inserted_date_time) as data_freshness
from {{ ref("stg_metrics__fact_lc") }}
