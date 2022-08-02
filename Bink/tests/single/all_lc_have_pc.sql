/*
 Test to ensure no duplicate transaction ids that are not net zero spends - i.e refunds
 
 Created By:     SP
 Created Date:   2022/07/19
 */


{{
    config(
        tags = ['business']
        ,error_if = '>100'
        ,warn_if = '>100'
    ) 
}}

WITH new_lc AS (
    SELECT *
    FROM
        {{ref('fact_loyalty_card_join')}}
    WHERE
        EVENT_TYPE = 'SUCCESS'
        AND CHANNEL LIKE '%barclays%'
        AND TIMEDIFF(
                    HOUR, EVENT_DATE_TIME, (
                        SELECT MAX(EVENT_DATE_TIME)
                        FROM {{ref('fact_loyalty_card_join')}}
                        )
                    ) < 24
)
  
SELECT
    USER_ID
FROM
    new_lc
WHERE
    USER_ID NOT IN (SELECT USER_ID FROM {{ref('fact_payment_account')}})