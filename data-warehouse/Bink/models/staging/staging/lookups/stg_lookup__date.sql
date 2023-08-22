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
with
    source as (select * from {{ source("LOOKUP", "DATE") }}),
    renaming as (
        select
            date(date) as date,
            year,
            quarter,
            month,
            monthname,
            dayofmonth,
            dayofweek,
            weekofyear,
            dayofyear,
            dayname,
            weekpart,
            daynumber,
            yearnumber,
            quarternumber,
            monthnumber,
            year_quarter,
            year_month
        from source
    )

select *
from renaming
