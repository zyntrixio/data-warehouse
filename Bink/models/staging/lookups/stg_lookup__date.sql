/*
Created by:         Aidan Summerville
Created date:       2022-04-22
Last modified by:   
Last modified date: 

Description:
    Stages the date table

Parameters:
    sources   - lookup.date

*/
with source as (

    select *
    from {{source('LOOKUP','DATE')}}
)


,renaming as (
SELECT  DATE(DATE) as DATE
       ,YEAR
       ,QUARTER
       ,MONTH
       ,MONTHNAME
       ,DAYOFMONTH
       ,DAYOFWEEK
       ,WEEKOFYEAR
       ,DAYOFYEAR
       ,DAYNAME
       ,WEEKPART
       ,DAYNUMBER
       ,YEARNUMBER
       ,QUARTERNUMBER
       ,monthNUMBER
       ,year_QUARTER
       ,year_month
          from source
)


select *
 from renaming 