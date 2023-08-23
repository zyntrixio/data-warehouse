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
with
datetable as (select * from {{ ref("stg_lookup__date") }}),

finacial as (
    select
        date,
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
        year_month,
        date_trunc(month, date) as start_of_month,
        last_day(date, month) as end_of_month,
        date_trunc(year, date) as start_of_year,
        last_day(date, year) as end_of_year,
        date_trunc(quarter, date) as start_of_quarter,
        last_day(date, quarter) as end_of_quarter,
        date_trunc(week, date) as start_of_week,
        last_day(date, week) as end_of_week,
        year(dateadd(month, 8, date)) as financial_year,
        quarter(dateadd(month, 8, date)) as financial_quarter,
        month(dateadd(month, 8, date)) as financial_month,
        weekofyear(dateadd(month, 8, date)) as financial_weekofyear,
        dayofyear(dateadd(month, 8, date)) as financial_dayofyear
    from datetable d
),

final as (
    select
        date,
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
        year_month,
        start_of_month,
        end_of_month,
        start_of_year,
        end_of_year,
        start_of_quarter,
        end_of_quarter,
        start_of_week,
        end_of_week,
        financial_year,
        financial_quarter,
        financial_month,
        financial_weekofyear,
        financial_dayofyear,
        financial_year::varchar||'-'|| financial_quarter::varchar as financial_year_quarter, -- noqa: disable=all
        financial_year::varchar||'-'|| financial_month::varchar as financial_year_month, -- noqa: disable=all
        dateadd(month, 8, start_of_year) as start_of_financial_year,
        dateadd(month, 8, end_of_year) as end_of_financial_year,
        dateadd(month, 8, start_of_quarter) as start_of_financial_quarter,
        dateadd(month, 8, end_of_quarter) as end_of_financial_quarter
    from finacial d
)

select *
from final
