/*
Created by:         Sam Pibworth
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the payment status table

Parameters:
    source_object      - HERMES.PAYMENT_CARD_PROVIDERSTATUSMAPPING
*/

WITH payment_status AS (
	SELECT
		ID
		,PROVIDER_ID
		,BINK_STATUS_CODE
		,PROVIDER_STATUS_CODE
	FROM
		{{ source('Hermes', 'PAYMENT_STATUS') }}
)

SELECT
	*
FROM
	payment_status
	