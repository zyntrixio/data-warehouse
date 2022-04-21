/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   
Last modified date: 

Description:
    Adds the ID_DELETD field to the user table

Parameters:
    ref_object      - stg_hermes__user
*/


WITH
transformed_user AS (
	SELECT
		*
		,FALSE :: boolean AS IS_DELETED
	FROM
		{{ ref('stg_hermes__user')}}
)

SELECT
	*
FROM
	transformed_user
	