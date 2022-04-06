WITH source as (

    select * 
    from {{ source('Bink', 'User') }}

),

renaming as (
    SELECT
    UID,
	SALT,
	APPLE,
	EMAIL,
	TWITTER,
	FACEBOOK,
	IS_STAFF,
	PASSWORD,
	CLIENT_ID,
	IS_ACTIVE,
	IS_TESTER,
	LAST_LOGIN,
	DATE_JOINED,
	EXTERNAL_ID,
	RESET_TOKEN,
	DELETE_TOKEN,
	IS_SUPERUSER,
	MARKETING_CODE_ID,
	MAGIC_LINK_VERIFIED
    FROM source
)

select * from source