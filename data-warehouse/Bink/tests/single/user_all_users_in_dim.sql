/*
Test to ensure all create user events have a matching user in dim_user

Created By:     SP
Created Date:   2022/07/12
*/

WITH new_users AS (
    SELECT *
    FROM {{ref('fact_user')}}
    WHERE EVENT_TYPE = 'CREATED'
    AND IS_MOST_RECENT = true
    AND TIMEDIFF(
                        HOUR, EVENT_DATE_TIME, (
                            SELECT MAX(EVENT_DATE_TIME)
                            FROM {{ref('fact_user')}}
                            )
                        ) < 24
)

SELECT *
FROM new_users
WHERE user_id NOT IN (SELECT user_id from {{ref('dim_user')}})
