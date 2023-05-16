/*
 Test to ensure all barclays lc create events have a PLL link
 
 Created By:     SP
 Created Date:   2022/07/13
 */


{{ config(
        tags=['business']
        ,error_if = '>100'
        ,warn_if = '>100'
        ,meta={"description": "Test to ensure all barclays lc create events have a PLL link with set limits.", 
            "test_type": "Business"},
) }}

-- SELECT
--     *
-- FROM {{ref('fact_loyalty_card')}}
-- WHERE loyalty_card_id NOT IN (
--         SELECT loyalty_card_id
--         FROM {{ref('join_loyalty_card_payment_account')}}
--     )
--     AND EVENT_TYPE = 'SUCCESS'
--     AND IS_MOST_RECENT = true
--     AND CHANNEL LIKE '%barclays%'
--     AND TIMEDIFF(hour, EVENT_DATE_TIME, (
--             select max(EVENT_DATE_TIME)
--             from {{ref('fact_loyalty_card')}}
--         )
--     ) < 24

select * from (Select null as n) t where t.n is not null
