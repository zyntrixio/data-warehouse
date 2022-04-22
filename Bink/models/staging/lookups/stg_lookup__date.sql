with source as (

    select *
    from {{source('LOOKUP','DATE')}}
)


,renaming as (
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
          from source
)


select *
 from renaming 