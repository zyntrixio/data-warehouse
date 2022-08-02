/*
 Test to monitor long delays whilst the payment account is in pending
 
 Created By:     SP
 Created Date:   2022/07/19
*/


{{
    config(
        tags = ['business']
        ,error_if = '>1000'
        ,warn_if = '>100'
    ) 
}}

WITH new_pa AS (
    SELECT *
    FROM {{ref('fact_payment_account')}}
    WHERE EVENT_TYPE = 'ADDED'
    AND TIMEDIFF(
                        HOUR, EVENT_DATE_TIME, (
                            SELECT MAX(EVENT_DATE_TIME)
                            FROM {{ref('fact_payment_account')}}
                            )
                        ) < 24
)

,wait_times AS (
    SELECT 
        pasc.EVENT_DATE_TIME AS PENDING_DT
        ,pa.EVENT_DATE_TIME AS CREATED_DT
        ,TIMEDIFF(MINUTE,CREATED_DT, PENDING_DT) AS WAIT_MINUTES_PENDING
    FROM new_pa pa
    LEFT JOIN
        {{ref('fact_payment_account_status_change')}} pasc
        ON pa.PAYMENT_ACCOUNT_ID = pasc.PAYMENT_ACCOUNT_ID
)
  
SELECT *
FROM wait_times
WHERE WAIT_MINUTES_PENDING > 10
  
  