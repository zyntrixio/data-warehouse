/*
Created by:         Sam Pibworth
Created date:       2022-04-08
Last modified by:   
Last modified date: 

Description:
    Stages the user table, containing data about current users

Parameters:
    source_object      - HERMES.USER
*/

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
		MAGIC_LINK_VERIFIED	:: varchar	 AS MAGIC_LINK_VERIFIED
	FROM
		current_users
)

SELECT
	*
FROM
	current_users_renamed