/*
 Created by:         Sam Pibworth
 Created date:       2022-06-23
 Last modified by:   
 Last modified date: 
 
 Description:
 Joining table, containing laid pll links from the scheme table and payment account table
 
 Parameters:
 ref_object      	- stg_hermes__SCHEME_SCHEMEACCOUNT
 				 	- stg_hermes__PAYMENT_ACCOUNT
 */

 
WITH
scheme AS (
	SELECT
		LOYALTY_CARD_ID,
		PARSE_JSON(pll_links) AS pll
	FROM {{ref('stg_hermes__SCHEME_SCHEMEACCOUNT')}}
	WHERE pll_links != '[]'
)

,scheme_plls AS (
	SELECT
		LOYALTY_CARD_ID,
		lf.value :"active_link"::boolean AS ACTIVE_LINK,
		lf.value :"id"::int AS PLL_LINK_ID
	FROM scheme,
		lateral flatten (input => pll) lf
)

,payment_accounts AS (
	SELECT
		PAYMENT_ACCOUNT_ID,
		PARSE_JSON(pll_links) AS pll
	FROM {{ref('stg_hermes__PAYMENT_ACCOUNT')}}
	WHERE pll_links != '[]'
)

,payment_account_plls AS (
	SELECT
		PAYMENT_ACCOUNT_ID,
		lf.value :"active_link"::boolean AS ACTIVE_LINK,
		lf.value :"id"::int AS PLL_LINK_ID
	FROM payment_accounts,
		lateral flatten (input => pll) lf
)

,intersecting_plls AS (
	SELECT PLL_LINK_ID
	FROM scheme_plls
	INTERSECT
	SELECT PLL_LINK_ID
	FROM payment_account_plls
)

,joined_links as (
	SELECT
		i.PLL_LINK_ID,
		s.LOYALTY_CARD_ID::varchar AS LOYALTY_CARD_ID,
		p.PAYMENT_ACCOUNT_ID::varchar AS PAYMENT_ACCOUNT_ID,
		CASE WHEN s.active_link AND p.active_link
			THEN TRUE
			ELSE FALSE
			END AS active_link
	FROM intersecting_plls i
	INNER JOIN scheme_plls s
		ON s.PLL_LINK_ID = i.PLL_LINK_ID
	INNER JOIN payment_account_plls p
		ON i.PLL_LINK_ID = p.PLL_LINK_ID
)

SELECT *
FROM joined_links