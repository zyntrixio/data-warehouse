/*
 Test to ensure no duplicate transaction ids that are not net zero spends - i.e refunds
 
 Created By:     SP
 Created Date:   2022/07/19
 */


{{ config(tags = ['business']) }}

WITH duplicate_ids AS
(
    SELECT
        TRANSACTION_ID
        ,COUNT(TRANSACTION_ID) c
        ,MAX(EVENT_DATE_TIME) edt
    FROM
        {{ref('fact_transaction')}}
    GROUP BY 1 
    HAVING c > 1
        AND TIMEDIFF(
                hour, edt, (
                            SELECT MAX(EVENT_DATE_TIME)
                            FROM {{ref('fact_transaction')}}
                        )
                ) < 24
)

,remove_discounts AS (
    SELECT
        TRANSACTION_ID
        ,SUM(SPEND_AMOUNT) s
    FROM {{ref('fact_transaction')}}
    WHERE
        TRANSACTION_ID IN (SELECT TRANSACTION_ID FROM duplicate_ids)
    GROUP BY
        TRANSACTION_ID
    HAVING
        s != 0
)

select * from remove_discounts