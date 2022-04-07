WITH
current_users as (
	SELECT
		*
	FROM
		{{ source('Bink', 'USER') }}
)

,current_users_renamed as (
	SELECT
		APPLE				:: varchar	 AS APPLE,
		CLIENT_ID			:: varchar	 AS CLIENT_ID,
		DATE_JOINED			:: datetime	 AS DATE_JOINED,
		DELETE_TOKEN		:: varchar	 AS DELETE_TOKEN,
		EMAIL				:: varchar	 AS EMAIL,
		EXTERNAL_ID			:: varchar	 AS EXTERNAL_ID,
		FACEBOOK			:: varchar	 AS FACEBOOK,
		ID					:: varchar	 AS ID,
		IS_ACTIVE			:: boolean	 AS IS_ACTIVE,
		IS_STAFF			:: boolean	 AS IS_STAFF,
		IS_SUPERUSER		:: boolean	 AS IS_SUPERUSER,
		IS_TESTER			:: boolean	 AS IS_TESTER,
		LAST_LOGIN			:: datetime	 AS LAST_LOGIN,
		MARKETING_CODE_ID	:: varchar	 AS MARKETING_CODE_ID,
		PASSWORD			:: varchar	 AS PASSWORD,
		RESET_TOKEN			:: varchar	 AS RESET_TOKEN,
		SALT				:: varchar	 AS SALT,
		TWITTER				:: varchar	 AS TWITTER,
		UID					:: varchar	 AS UID,
		MAGIC_LINK_VERIFIED	:: varchar	 AS MAGIC_LINK_VERIFIED,
		FALSE				:: boolean	 AS IS_DELETED
	FROM
		current_users
)

,deleted_ids as (
	SELECT
		INSTANCE_ID as UID,
		MAX(CREATED) as CREATED,
		MAX(ID) as ID,
		CHANGE_TYPE
	FROM
		{{ source('Bink', 'HISTORY_USER') }}
	WHERE
		CHANGE_TYPE = 'delete'
	GROUP BY
		1,
		4
	order by
		INSTANCE_ID
)

,deleted_user_records as (
	SELECT
		parse_json(hu.body) as BODY
	FROM
		{{ source('Bink', 'HISTORY_USER') }} hu
  	INNER JOIN
	  	deleted_ids ids ON hu.id = ids.id
)

,parsed_deleted_user_records as (
	SELECT
		body:apple			 	:: varchar	 AS APPLE,
		body:client			 	:: varchar	 AS CLIENT_ID,
		body:date_joined		:: datetime	 AS DATE_JOINED,
		body:delete_token		:: varchar	 AS DELETE_TOKEN,
		body:email			 	:: varchar	 AS EMAIL,
		body:external_id		:: varchar	 AS EXTERNAL_ID,
		body:facebook			:: varchar	 AS FACEBOOK,
		body:id			 		:: varchar	 AS ID,
		body:is_active			:: boolean	 AS IS_ACTIVE,
		body:is_staff			:: boolean	 AS IS_STAFF,
		body:is_superuser		:: boolean	 AS IS_SUPERUSER,
		body:is_tester			:: boolean	 AS IS_TESTER,
		body:last_login			:: datetime	 AS LAST_LOGIN,
		body:marketing_code		:: varchar	 AS MARKETING_CODE_ID,
		body:password			:: varchar	 AS PASSWORD,
		body:reset_token		:: varchar	 AS RESET_TOKEN,
		body:salt			 	:: varchar	 AS SALT,
		body:twitter			:: varchar	 AS TWITTER,
		body:uid			 	:: varchar	 AS UID,
		NULL 					:: varchar	 AS MAGIC_LINK_VERIFIED,
		TRUE					:: boolean	 AS IS_DELETED

	FROM deleted_user_records
)

,joined_user_records as (
	SELECT
		*
	FROM
		parsed_deleted_user_records
	WHERE 
        ID not in
		(
			SELECT ID
			FROM current_users_renamed
		)
	UNION ALL
	SELECT
		*
	FROM
		current_users_renamed
)

SELECT
	*
FROM
	joined_user_records