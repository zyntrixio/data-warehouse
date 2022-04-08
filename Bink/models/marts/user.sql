/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   
Last modified date: 

Description:
    Unions the user and historical_user tables, ensuring that any duplicate IDs are removed. In this instance, the current
    user record is included and the historical record is not.

Parameters:
    ref_object      - transformed_user
    ref_object      - transformed_histuser
*/

WITH
hist_users as (
    SELECT *
    FROM {{ ref('transformed_histuser')}}
)

,users as (
    SELECT *
    FROM {{ ref('transformed_user')}}
)

,joined_user_records as (
	SELECT
		*
	FROM
		hist_users
	WHERE 
        ID not in
		(
			SELECT ID
			FROM users
		)
	UNION ALL
	SELECT
		*
	FROM
		users
)

SELECT
    *
FROM
    joined_user_records