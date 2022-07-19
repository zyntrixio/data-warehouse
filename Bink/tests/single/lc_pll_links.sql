/*
 Test to ensure all lc create event shave a corresponding create user event
 
 Created By:     SP
 Created Date:   2022/07/13
 */


{{ config(tags = ['business']) }}

SELECT
    *
FROM "DEV"."BINK"."FACT_LOYALTY_CARD_JOIN"
WHERE loyalty_card_id NOT IN (
        SELECT loyalty_card_id
        FROM "DEV"."BINK"."JOIN_LOYALTY_CARD_PAYMENT_ACCOUNT"
    )
    AND EVENT_TYPE = 'SUCCESS'
    AND IS_MOST_RECENT = true
    AND CHANNEL LIKE '%barclays%'
    AND TIMEDIFF(hour, EVENT_DATE_TIME, (
            select max(EVENT_DATE_TIME)
            from "DEV"."BINK"."FACT_LOYALTY_CARD_JOIN"
        )
    ) < 24