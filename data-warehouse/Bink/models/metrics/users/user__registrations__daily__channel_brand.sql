/*
Created by:         Christopher Mitchell
Created date:       2023-05-23
Last modified by:   
Last modified date: 

Description:
    CUMULATIVE AND PERIOD METRICS ON USER REGISTRATION AND DEREGISTRATION

Parameters:
    source_object       - user_trans
                        - src__dim_date
*/

WITH fact_usr AS (
    SELECT *
    FROM {{ ref('users_trans') }})

   , dim_date AS (
    SELECT *
    FROM {{ ref('src__dim_date') }}
    WHERE date >= (
        SELECT MIN(DATE(from_date))
        FROM fact_usr)
      AND date <= CURRENT_DATE())

   , usr_staging AS (
    SELECT d.date
         , u.channel
         , u.brand
         , COALESCE(COUNT(CASE WHEN event_type = 'CREATED' THEN 1 END), 0) AS daily_registrations  -- WHEN CREATE EVENT
         , COALESCE(COUNT(CASE WHEN event_type = 'DELETED' THEN 1 END), 0) AS daily_deregistrations-- WHEN DELETE EVENT
    FROM fact_usr u
             LEFT JOIN dim_date d
                       ON d.date = DATE(u.from_date)
    GROUP BY d.date, u.channel, u.brand)

   , usr_staging_snap AS (
    SELECT d.date
         , u.channel
         , u.brand
         , COALESCE(COUNT(CASE WHEN event_type = 'CREATED' THEN 1 END), 0) AS snap_user_registrations
         , COALESCE(COUNT(CASE WHEN event_type = 'DELETED' THEN 1 END), 0) AS snap_user_deregistrations
    FROM fact_usr u
             LEFT JOIN dim_date d
                       ON d.date <= DATE(from_date) AND d.date > DATE(u.to_date)
    GROUP BY d.date, u.channel, u.brand)

   , combine_all AS (
    SELECT COALESCE(a.date, s.date)                 AS date
         , COALESCE(a.channel, s.channel)           AS channel
         , COALESCE(a.brand, s.brand)               AS brand
         -- AUTH_TYPES
         , COALESCE(a.daily_registrations, 0)       AS daily_registrations
         , COALESCE(a.daily_deregistrations, 0)     AS daily_deregistrations
         , COALESCE(s.snap_user_registrations, 0)   AS snap_user_registrations
         , COALESCE(s.snap_user_deregistrations, 0) AS snap_user_deregistrations
    FROM usr_staging a
             FULL OUTER JOIN usr_staging_snap s
                             ON a.date = s.date AND a.brand = s.brand)

   , rename AS (
    SELECT date
         , channel
         , brand
         , daily_registrations       AS usr001__daily_registrations_period
         , daily_deregistrations     AS usr002__daily_deregistrations_period
         , snap_user_registrations   AS usr003__daily_registrations_cumulative
         , snap_user_deregistrations AS usr004__daily_deregistrations_cumulative
    FROM combine_all)

SELECT *
FROM rename