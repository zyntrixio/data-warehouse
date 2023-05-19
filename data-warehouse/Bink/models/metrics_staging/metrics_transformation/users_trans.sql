/*
Created by:         Christopher Mitchell
Created date:       2023-05-
Last modified by:   
Last modified date: 

Description:
    Channel table, which relates to the user_client tables in from user_facts, and to channel in events

Parameters:
    ref_object      - fact_user
					- dim_user?
*/

WITH source AS (SELECT
    EVENT_ID,
    EVENT_DATE_TIME,
    USER_ID,
    EVENT_TYPE,
    IS_MOST_RECENT,
    ORIGIN,
    CHANNEL,
    BRAND,
    EXTERNAL_USER_REF,
    INSERTED_DATE_TIME,
    UPDATED_DATE_TIME
FROM
    PROD.BINK_SECURE.FACT_USER
)

,join_one AS (
    SELECT
        s.EVENT_ID,
        s.EVENT_DATE_TIME,
        s.USER_ID,
        s.EVENT_TYPE,
        s.IS_MOST_RECENT,
        s.ORIGIN,
        s.CHANNEL,
        s.BRAND,
        s.EXTERNAL_USER_REF,
        s.INSERTED_DATE_TIME,
        s.UPDATED_DATE_TIME
    FROM
        source s
)

select * from join_one limit 100;