/*
 Test to ensure all lc create event shave a corresponding create user event
 
 Created By:     SP
 Created Date:   2022/07/13
 */


{{ config(tags = ['business']) }}

SELECT
    *
FROM {{ref('fact_loyalty_card_join')}}
WHERE loyalty_card_id NOT IN (
        SELECT loyalty_card_id
        FROM {{ref('join_loyalty_card_payment_account')}}
    )
    AND EVENT_TYPE = 'SUCCESS'
    AND IS_MOST_RECENT = true
    AND CHANNEL LIKE '%barclays%'
    AND TIMEDIFF(hour, EVENT_DATE_TIME, (
            select max(EVENT_DATE_TIME)
            from {{ref('fact_loyalty_card_join')}}
        )
    ) < 24