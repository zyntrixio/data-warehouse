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

WITH payment_card AS (
	SELECT
		ID
		,URL
		,NAME
		,SLUG
		,TYPE
		,SYSTEM
		,IS_ACTIVE
		,INPUT_LABEL
		,SCAN_MESSAGE
		,TOKEN_METHOD
		,FORMATTED_IMAGES
	FROM
		{{ source('Hermes', 'PAYMENT_CARD') }}
)

SELECT
	*
FROM
	payment_card
	