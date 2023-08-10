/*
 Test to ensure all duplicate transactions are not visable in the exported transactions
 
 Created By:     Chris Mitchell
 Created Date:   2023-08-09
 force git update
 */

{{ config(
        tags=['business']
        ,error_if = '>10'
        ,warn_if = '>1'
        ,meta={"description": "Test to ensure all duplicate transactions are not visable in the exported transactions", 
            "test_type": "Business"},
) }}

SELECT t1.transaction_id, t1.event_id
FROM {{ ref('fact_transaction_secure') }} t1
         JOIN {{ ref('fact_transaction_secure') }} t2 ON t1.transaction_id = t2.transaction_id
WHERE t1.duplicate_transaction = 'True'
  AND t1.transaction_id IS NOT NULL
  AND t1.event_id <> t2.event_id