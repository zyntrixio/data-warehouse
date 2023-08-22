/*
Created by:         Aidan Summerville
Created date:       2022-04-21
Last modified by:   
Last modified date: 

Description:
    Stages the base table the indovdual loyalty plans

Parameters:
    source_object      - Hermes.SCHEME_SCHEME
*/
with
    source as (select * from {{ source("Hermes", "SCHEME_SCHEME") }}),
    renaming as (

        select
            id as loyalty_plan_id,
            has_points,
            join_url,
            url,
            _airbyte_emitted_at,
            _airbyte_scheme_scheme_hashid,
            company as loyalty_plan_company,
            itunes_url,
            authorisation_required,
            barcode_prefix,
            barcode_type,
            card_number_prefix,
            colour,
            enrol_incentive,
            play_store_url,
            plan_popularity,
            digital_only,
            plan_name,
            slug as loyalty_plan_slug,
            text_colour,
            tier as loyalty_plan_tier,
            max_points_value_length,
            plan_description,
            plan_summary,
            formatted_images,
            ios_scheme,
            linking_support,
            plan_name_card as loyalty_plan_name_card,
            plan_register_info,
            _airbyte_normalized_at,
            barcode_regex,
            company_url,
            join_t_and_c,
            name as loyalty_plan_name,
            scan_message,
            identifier,
            android_app_id,
            barcode_redeem_instructions,
            card_number_regex,
            forgotten_password_url,
            has_transactions,
            link_account_text,
            point_name,
            category_id as loyalty_plan_category_id,
            secondary_colour,
            transaction_headers,
            _airbyte_ab_id
        from source

    )

select *
from renaming
