/*
 Test to ensure all lc create event shave a corresponding create user event
 
 Created By:     SP
 Created Date:   2022/07/13
 */


{{ config(
    tags=['business']
    ,meta={"description": "This test ensures there are not more deleted events than created", "test_type": "Business"},
) }}

with joins AS (
    SELECT *
    FROM {{ ref('fact_loyalty_card_join')}}
    WHERE event_type = 'SUCCESS'
        AND is_most_recent = true
),

removals AS (
    SELECT r.LOYALTY_CARD_ID
    FROM {{ ref('fact_loyalty_card_removed')}} r
        LEFT JOIN joins j ON j.LOYALTY_CARD_ID = r.LOYALTY_CARD_ID
)

SELECT loyalty_card_id
FROM {{ ref('fact_loyalty_card_removed')}}
MINUS
SELECT loyalty_card_id
FROM removals
