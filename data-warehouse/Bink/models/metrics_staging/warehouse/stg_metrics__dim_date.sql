with
    source as (select * from {{ ref("dim_date") }}),
    renamed as (
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
            financial_year_quarter,
            financial_year_month,
            start_of_financial_year,
            end_of_financial_year,
            start_of_financial_quarter,
            end_of_financial_quarter
        from source
    )

select *
from renamed
