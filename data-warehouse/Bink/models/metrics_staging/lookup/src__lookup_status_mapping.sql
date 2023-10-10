/*
Created by:         Anand Bhakta
Created date:       2023-02-05
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    Source file for status mapping for scheme account status
Parameters:
    source_object       - source("STAGING", "STG_LOOKUP__SCHEME_ACCOUNT_STATUS")
*/
with
source as (
    select * from {{ source("STAGING", "STG_LOOKUP__SCHEME_ACCOUNT_STATUS") }}
)

select *
from source
