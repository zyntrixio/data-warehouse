/*
Created by:         Aidan Summerville
Created date:       2022-04-22
Last modified by:   Christopher Mitchell
Last modified date: 05-06-2023

Description:
	The output Dimension table for dates/calendar

Parameters:
    ref_object      - stg_lookup__date

*/


with datetable as (
    select * 
    from {{ref('stg_lookup__date')}}
)


,finacial as (
SELECT  Date
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
       ,date_trunc(month,date)            AS start_of_month
       ,last_day(date,month)              AS end_of_month
       ,date_trunc(year,date)             AS start_of_year
       ,last_day(date,year)               AS end_of_year
       ,date_trunc(quarter,date)          AS start_of_quarter
       ,last_day(date,quarter)            AS end_of_quarter
       ,date_trunc(week,date)             AS start_of_week
       ,last_day(date,week)               AS end_of_week
       ,YEAR(dateadd(month,8,date))       AS FINANCIAL_YEAR
       ,QUARTER(dateadd(month,8,date))    AS FINANCIAL_QUARTER
       ,MONTH(dateadd(month,8,date))      AS FINANCIAL_MONTH
       ,WEEKOFYEAR(dateadd(month,8,date)) AS FINANCIAL_WEEKOFYEAR
       ,DAYOFYEAR(dateadd(month,8,date))  AS FINANCIAL_DAYOFYEAR     
    from datetable d
)


,final as (
    SELECT  Date
        , YEAR
        , QUARTER
        , MONTH
        , MONTHNAME
        , DAYOFMONTH
        , DAYOFWEEK
        , WEEKOFYEAR
        , DAYOFYEAR
        , DAYNAME
        , WEEKPART
        , DAYNUMBER
        , YEARNUMBER
        , QUARTERNUMBER
        , monthNUMBER
        , year_QUARTER
        , year_month
        , start_of_month
        , end_of_month
        , start_of_year
        , end_of_year
        , start_of_quarter
        , end_of_quarter
        , start_of_week
        , end_of_week
        , FINANCIAL_YEAR
        , FINANCIAL_QUARTER
        , FINANCIAL_MONTH
        , FINANCIAL_WEEKOFYEAR
        , FINANCIAL_DAYOFYEAR    
        , FINANCIAL_YEAR::varchar||'-'||FINANCIAL_QUARTER::varchar  AS FINANCIAL_YEAR_QUARTER
        , FINANCIAL_YEAR::varchar||'-'||FINANCIAL_MONTH::varchar    AS FINANCIAL_YEAR_month  
        , dateadd(month,8,start_of_year)                            AS start_of_financial_year
        , dateadd(month,8,end_of_year)                              AS end_of_financial_year
        , dateadd(month,8,start_of_quarter)                         AS start_of_financial_quarter
        , dateadd(month,8,end_of_quarter)                           AS end_of_financial_quarter
    from finacial d
)
select *
from final