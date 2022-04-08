WITH
transformed_user AS (
	SELECT
		*
		,FALSE :: boolean AS IS_DELETED
	FROM
		{{ ref('stg_user')}}
)

SELECT
	*
FROM
	transformed_user
	