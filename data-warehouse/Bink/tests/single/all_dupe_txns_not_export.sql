/*
 Test to ensure all duplicate transactions are not visable in the exported transactions
 
 Created By:     Chris Mitchell
 Created Date:   2023-08-09

 */

{{ config(
        tags=['business']
        ,error_if = '>0'
        ,meta={"description": "Test to ensure all duplicate transactions are not visable in the exported transactions", 
            "test_type": "Business"},
) }}

WITH dupes AS (
    SELECT * FROM {{ ref('fact_transaction_secure') }} WHERE duplicate_transaction = TRUE)

   , exports AS (
    SELECT * FROM {{ ref('fact_transaction_secure') }}  WHERE duplicate_transaction = FALSE)

   , test_outputs AS (
    SELECT d.transaction_id
    FROM dupes d
             INTERSECT SELECT e.transaction_id FROM exports e)

SELECT *
FROM test_outputs