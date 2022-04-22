/*
Created by:         Aidan Summerville
Created date:       2022-04-22
Last modified by:   
Last modified date: 

Description:
	The output Dimension table for dates/calendar

Parameters:
    ref_object      - stg_lookup__date

*/


with datetable as (
    select * 
    from {{ref('stg_lookup__date')}}
)


,final as (
    select 
    DATE,
          YEAR,
          QUARTER,
          MONTH,
          MONTHNAME,
          DAYOFMONTH,
          DAYOFWEEK,
          WEEKOFYEAR,
          DAYOFYEAR,
          DAYNAME,
        WEEKPART,
          DAYNUMBER,
          YEARNUMBER,
           QUARTERNUMBER,
           monthNUMBER
          , year_QUARTER
          , year_month  
    from datetable d
)

select *
from final