/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the lookup table of the loyalty plan categorys

Parameters:
    source_object      - Hermes.SCHEME_CATEGORY
*/

WITH
source  as (
	SELECT	*
	FROM {{ source('Hermes', 'SCHEME_CATEGORY') }}
)


,renaming  as (



select 
_AIRBYTE_NORMALIZED_AT,
_AIRBYTE_AB_ID,
NAME as LOYALTY_PLAN_CATEGORY,
_AIRBYTE_EMITTED_AT,
ID as LOYALTY_PLAN_CATEGORY_ID,
_AIRBYTE_SCHEME_CATEGORY_HASHID
from source
	
)

SELECT
	*
FROM renaming