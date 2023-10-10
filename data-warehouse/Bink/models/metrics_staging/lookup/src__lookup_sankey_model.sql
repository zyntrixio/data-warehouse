/*
Created by:         Anand Bhakta
Created date:       2023-02-05
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    Source file for LBG Sankey Funnel Mode
Parameters:
    source_object       - source("RAW_BINK_LOOKUP", "SANKEY_MODEL")
*/
with source as (select * from {{ source("RAW_BINK_LOOKUP", "SANKEY_MODEL") }})

select *
from source
