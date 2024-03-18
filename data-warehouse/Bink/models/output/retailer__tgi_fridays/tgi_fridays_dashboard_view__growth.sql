/*
Created by:         Christopher Mitchell
Created date:       2024-03-14
Last modified by:   
Last modified date: 

Description:
    Datasource to produce tableau dashboard for The Works
Parameters:
    source_object       - lc__links_joins__monthly_retailer
                        - trans__trans__monthly_retailer
                        - trans__avg__monthly_retailer
                        - user__transactions__monthly_retailer
*/
with
lc_metric as (
    select
        *,
        'JOINS' as category
    from {{ ref("lc__links_joins__monthly_retailer__growth") }}
    where loyalty_plan_company = 'TGI Fridays'
),

txn_metrics as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__trans__monthly_retailer__growth") }}
    where loyalty_plan_company = 'TGI Fridays'
),

txn_avg as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__avg__monthly_retailer__growth") }}
    where loyalty_plan_company = 'TGI Fridays'
),

user_metrics as (
    select
        *,
        'USERS' as category
    from {{ ref("user__transactions__monthly_retailer__growth") }}
    where loyalty_plan_company = 'TGI Fridays'
),

pll_metrics as (
    select
        *,
        'JOINS' as category
    from {{ ref("lc__pll__monthly_retailer__growth") }}
    where loyalty_plan_company = 'TGI Fridays'
),

combine_all as (
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        lc347__successful_loyalty_card_joins__monthly_retailer__count__growth,
        lc351__successful_loyalty_card_links__monthly_retailer__dcount_user__growth,
        lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage__growth,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
        null as u107_active_users__retailer_monthly__dcount_uid__growth,
        null as u108_active_users_retailer_monthly__cdcount_uid__growth,
        null as t011__txns__monthly_retailer__dcount__growth,
        null as t012__refund__monthly_retailer__dcount__growth,
        null as t009__spend__monthly_retailer__sum__growth,
        null as t014__aov__monthly_retailer__avg__growth,
        null as t015__arpu__monthly_retailer__avg__growth,
        null as t016__atf__monthly_retailer__avg__growth,
        null as t026__txns_and_dupes__monthly_retailer__dcount__growth,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage__growth,
        null as t017__duplicate_txn__monthly_retailer__dcount__growth
    from lc_metric
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count__growth__growth,
        null as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user__growth,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage__growth,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
        null as u107_active_users__retailer_monthly__dcount_uid__growth,
        null as u108_active_users_retailer_monthly__cdcount_uid__growth,
        t011__txns__monthly_retailer__dcount__growth,
        t012__refund__monthly_retailer__dcount__growth,
        t009__spend__monthly_retailer__sum__growth,
        null as t014__aov__monthly_retailer__avg__growth,
        null as t015__arpu__monthly_retailer__avg__growth,
        null as t016__atf__monthly_retailer__avg__growth,
        t026__txns_and_dupes__monthly_retailer__dcount__growth,
        t019__duplicate_txn_per_txn__monthly_retailer__percentage__growth,
        t017__duplicate_txn__monthly_retailer__dcount__growth
    from txn_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count__growth__growth,
        null as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user__growth,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage__growth,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
        null as u107_active_users__retailer_monthly__dcount_uid__growth,
        null as u108_active_users_retailer_monthly__cdcount_uid__growth,
        null as t011__txns__monthly_retailer__dcount__growth,
        null as t012__refund__monthly_retailer__dcount__growth,
        null as t009__spend__monthly_retailer__sum__growth,
        t014__aov__monthly_retailer__avg__growth,
        t015__arpu__monthly_retailer__avg__growth,
        t016__atf__monthly_retailer__avg__growth,
        null as t026__txns_and_dupes__monthly_retailer__dcount__growth,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage__growth,
        null as t017__duplicate_txn__monthly_retailer__dcount__growth
    from txn_avg
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count__growth__growth,
        null as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user__growth,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage__growth,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
        u107_active_users__retailer_monthly__dcount_uid__growth,
        u108_active_users_retailer_monthly__cdcount_uid__growth,
        null as t011__txns__monthly_retailer__dcount__growth,
        null as t012__refund__monthly_retailer__dcount__growth,
        null as t009__spend__monthly_retailer__sum__growth,
        null as t014__aov__monthly_retailer__avg__growth,
        null as t015__arpu__monthly_retailer__avg__growth,
        null as t016__atf__monthly_retailer__avg__growth,
        null as t026__txns_and_dupes__monthly_retailer__dcount__growth,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage__growth,
        null as t017__duplicate_txn__monthly_retailer__dcount__growth
    from user_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count__growth__growth,
        null as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user__growth,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage__growth,
        lc201__loyalty_card_active_pll__monthly_retailer__pit__growth,
        null as u107_active_users__retailer_monthly__dcount_uid__growth,
        null as u108_active_users_retailer_monthly__cdcount_uid__growth,
        null as t011__txns__monthly_retailer__dcount__growth,
        null as t012__refund__monthly_retailer__dcount__growth,
        null as t009__spend__monthly_retailer__sum__growth,
        null as t014__aov__monthly_retailer__avg__growth,
        null as t015__arpu__monthly_retailer__avg__growth,
        null as t016__atf__monthly_retailer__avg__growth,
        null as t026__txns_and_dupes__monthly_retailer__dcount__growth,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage__growth,
        null as t017__duplicate_txn__monthly_retailer__dcount__growth
    from pll_metrics
)

select *
from combine_all
