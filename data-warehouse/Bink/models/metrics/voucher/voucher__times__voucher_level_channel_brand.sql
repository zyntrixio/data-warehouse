/*
Created by:         Christopher Mitchell 
Created date:       2023-07-04
Last modified by:   
Last modified date: 

Description:
    todo
Parameters:
    source_object       - voucher_trans
*/

WITH voucher_trans AS (
    SELECT *
    FROM {{ ref('voucher_trans') }})

,metrics AS (
    SELECT DISTINCT
        CHANNEL
        ,BRAND
        ,loyalty_plan_company
        ,loyalty_plan_name
        ,state
        ,earn_type
        ,voucher_code
        ,REDEMPTION_TRACKED
        ,DATE_REDEEMED
        ,DATE_ISSUED
        ,EXPIRY_DATE
        ,TIME_TO_REDEMPTION AS V007__time_to_redemption__voucher_level__SUM
        ,days_left_on_vouchers AS V008__days_left_on_voucher__voucher_level__SUM
    FROM
        voucher_trans

)

SELECT * FROM metrics
