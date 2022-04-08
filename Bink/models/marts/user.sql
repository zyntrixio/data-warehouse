WITH
hist_users as (
    SELECT *
    FROM {{ ref('transformed_histuser')}}
)

,users as (
    SELECT *
    FROM {{ ref('stg_user')}}
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