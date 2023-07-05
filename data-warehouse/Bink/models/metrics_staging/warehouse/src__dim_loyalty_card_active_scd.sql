WITH source AS (

    SELECT * FROM {{ source('BINK', 'DIM_LOYALTY_CARD_ACTIVE_SCD') }}

),

renamed AS (

    SELECT
      loyalty_card_id
     , user_id
     , channel
     , brand
     , loyalty_plan_company
     , loyalty_plan_name
     , removed
     , valid_from
     , valid_to
    FROM source

)
SELECT *
FROM renamed