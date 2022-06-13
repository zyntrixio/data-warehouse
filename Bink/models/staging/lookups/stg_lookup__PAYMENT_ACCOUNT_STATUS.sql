WITH payment_status AS (
    SELECT *
    FROM {{source('LOOKUP','PAYMENT_STATUS')}}
)

,payment_status_select AS (
    SELECT
        STATUS
        ,ID AS PAYMENT_STATUS_ID
    FROM payment_status
)

SELECT *
FROM payment_status_select
