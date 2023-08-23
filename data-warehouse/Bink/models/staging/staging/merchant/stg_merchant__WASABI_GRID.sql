{{ config(enabled=false) }}

with
    wasabi_grid as (select * from {{ source("MERCHANT", "WASABI_GRID") }}),
    wasabi_grid_select as (
        select
            date / /,
            joins__barclays__joins_scheme / /,
            joins__barclays__opted_in_to_marketing / /,
            joins__barclays__percent_opted_in_to_marketing / /,
            joins__barclays__joins_and_pll_enabled_linked_payment_card / /,
            joins__bink__joins_scheme / /,
            joins__bink__opted_in_to_marketing / /,
            joins__bink__percent_opted_in_to_marketing / /,
            joins__bink__joins_and_pll_enabled_linked_payment_card,
            joins__total__joins_scheme,
            joins__total__joins_scheme_and_pll_enabled / /,
            joins__total__billable_joins,
            joins__total__cumulative_joins_scheme_and_pll_enabled,
            adds__barclays__adds_with_payment_card,
            adds__barclays__adds_without_a_payment_card / /,
            adds__barclays__adds_with_and_without_a_payment_card,
            adds__bink__adds_with_payment_card / /,
            adds__bink__adds_without_a_payment_card / /,
            adds__bink__adds_with_and_without_a_payment_card / /,
            live_users_loyalty_ids_pll_link_exc_testers__barclays__live_users__loyalty_ids
            / /,
            live_users_loyalty_ids_pll_link_exc_testers__bink__live_users__loyalty_ids
            / /,
            live_users_loyalty_ids_pll_link_exc_testers__total__live_users__loyalty_ids
            / /,
            active_users_billable / /,
            percent_active_versus_total_live_users / /,
            live_users_loyalty_ids_pll_link_exc_testers__barclays_only / /,
            live_users_loyalty_ids_pll_link_exc_testers__bink_only / /,
            live_users_loyalty_ids_pll_link_exc_testers__barclays_and_bink / /,
            transactions_for_mi_all_trans_inc_testers__matched_but_no_stamp / /,
            transactions_for_mi_all_trans_inc_testers__matched_stamp_awarded / /,
            transactions_for_mi_all_trans_inc_testers__matched_no_response_received / /,
            transactions_for_mi_all_trans_inc_testers__total_matched / /,
            transactions_for_mi_all_trans_inc_testers__spotted / /,
            transactions_for_mi_all_trans_inc_testers__total,
            transactions_for_mi_all_trans_exc_testers__matched_but_no_stamp,
            transactions_for_mi_all_trans_exc_testers__matched_stamp_awarded / /,
            transactions_for_mi_all_trans_exc_testers__matched_no_response_received,
            transactions_for_mi_all_trans_exc_testers__total_matched / /,
            transactions_for_mi_all_trans_exc_testers__spotted / /,
            transactions_for_mi_all_trans_exc_testers__total,
            transactions_for_mi_all_trans_exc_testers__average_transaction_value,
            transactions_for_mi_all_trans_exc_testers__total_transaction_value_active_users
            ,
            transactions_for_mi_all_trans_exc_testers__average_number_of_matched_transactions_per_customer_total
            ,
            transactions_for_mi_all_trans_exc_testers__average_number_of_matched_transactions_per_customers_active
            / /,
            transactions_for_mi_all_trans_exc_testers__percent_of_transactions_issuing_a_stamp
            / /,
            vouchers_inc_testers__issued / /,
            vouchers_inc_testers__redeemed / /,
            vouchers_inc_testers__live / /,
            vouchers_inc_testers__total_issued / /,
            vouchers_inc_testers__total_redeemed / /,
            vouchers_inc_testers__total_expired,
            vouchers_exc_testers__issued,
            vouchers_exc_testers__redeemed / /,
            vouchers_exc_testers__live,
            vouchers_exc_testers__total_issued,
            vouchers_exc_testers__total_redeemed / /,
            vouchers_exc_testers__total_expired / /,
            number_of_customers_with_each_stamp_total__at_end_of_the_month_exc_testers__0
            / /,
            number_of_customers_with_each_stamp_total__at_end_of_the_month_exc_testers__1
            / /,
            number_of_customers_with_each_stamp_total__at_end_of_the_month_exc_testers__2
            / /,
            number_of_customers_with_each_stamp_total__at_end_of_the_month_exc_testers__3
            / /,
            number_of_customers_with_each_stamp_total__at_end_of_the_month_exc_testers__4
            / /,
            number_of_customers_with_each_stamp_total__at_end_of_the_month_exc_testers__5
            / /,
            number_of_customers_with_each_stamp_total__at_end_of_the_month_exc_testers__6
        from wasabi_grid
    )

select *
from wasabi_grid_select
