{% docs __overview__ %}
# Overview
Wite something here to help people navigate the docs maybe even leave a link to something [here](https://ichef.bbci.co.uk/onesport/cps/976/cpsprodpb/671B/production/_128859362_639f0e6ef239607844f40817e5a3829730f89f01.jpg)

---
# Documenting Tests
Below is a list of all tests implemented in the DWH. This will provide a summary of each test and what it aims to target. To see where these tests are executed please use the **dashboard view** where you can click into each model and see what tests are executed on that model. Alternatively, navigate to the **project view** to see the customised test and here you can see on what models these tests are executed on *Note: this does not include thr preset tests (unique, not_null, relationships, accepted_values)*.

---
## Preset Tests
### Unique
    Check if values in a collumn are unique

### Not Null
    Check if there are Null values in a collumn

### Relationship
    Checks if every value in the collumn is present in another tables ["to"] collumm ["field"]

### Accepted Values
    Checks if values in the collumn are present in the accepted values 

---
## Source Tests ##
    Source test are run by package elementary - to be documented later

---
## Business Tests ##
### lc_more_created_removed
    This test ensures there are not more deleted events than created
### user_all_events_have_user_create_events
    Test to ensure all event tables with users have a corresponding create user event - this is redundant for now as users created before events(could this be for lloyds only).
### user_consecutive_creates
    Test to ensure no create user events are followed by another create user event in last 24 hours.
### lc_pll_links
    Test to ensure all barclays lc create events have a PLL link with set limits.
### sd_daily_spike_fact_user
    Generic test to ensure the number of events in the past day isn't outside a given multiple of the standard deviation beyond the median.
### more_created_deleted_fact_user_secure
    Generic Test to ensure sum of delete and create events is not less than 0 or greater than 1.
### all_lc_have_pc
    Test to ensure all active Barcalys loyalty cards are linked to a payment card.
### all_events_parsed
    Check if all events generated in raw make there way to a fact table in prod.
### sd_daily_spike_fact_transaction
    Generic test to ensure the number of events in the past day isn't outside a given multiple of the standard deviation beyond the median.
### user_all_users_in_dim
    Test to ensure all create user events have a matching user in dim_user.
### sd_daily_spike_fact_loyalty_card_status_change
    Generic test to ensure the number of events in the past day isn't outside a given multiple of the standard deviation beyond the median.
### pc_time_pending
    Test to monitor long delays (10 mins) whilst the payment account is in pending with set limits.
### lc_move_to_error_state
    Test to ensure all lc within the last day are not ending in error states with set limits.
### pc_repeat_fingerprints
    Test to ensure no duplicate fingerprints for a payment accounts created in the last day.
### sd_daily_spike_fact_payment_account
    Generic test to ensure the number of events in the past day isn't outside a given multiple of the standard deviation beyond the median.





{% enddocs %}
