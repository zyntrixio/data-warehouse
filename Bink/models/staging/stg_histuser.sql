WITH hist_users AS (
	SELECT
		*
	FROM
		{{ source('Bink', 'HISTORY_USER') }}
)

SELECT
	*
FROM
	hist_users
	