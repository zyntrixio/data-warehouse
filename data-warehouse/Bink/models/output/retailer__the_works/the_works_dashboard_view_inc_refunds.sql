/*
Created by:         Christopher Mitchell
Created date:       2023-07-05
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
    from {{ ref("lc__links_joins__monthly_retailer") }}
    where loyalty_plan_company = 'The Works'
),

txn_metrics as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__trans__monthly_retailer") }}
    where loyalty_plan_company = 'The Works'
),

txn_avg as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__avg__monthly_retailer") }}
    where loyalty_plan_company = 'The Works'
),

user_metrics as (
    select
        *,
        'USERS' as category
    from {{ ref("user__transactions__monthly_retailer") }}
    where loyalty_plan_company = 'The Works'
),

pll_metrics as (
    select
        *,
        'JOINS' as category
    from {{ ref("lc__pll__monthly_retailer") }}
    where loyalty_plan_company = 'The Works'
),

combine_all as (
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        lc347__successful_loyalty_card_joins__monthly_retailer__count,
        lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u112_active_users_inc_refunds__retailer_monthly__dcount_uid,
        null as u111_active_users_inc_refunds__retailer_monthly__cdcount_uid,
        null as t025__txns_and_refunds__monthly_retailer__dcount,
        null as t020__net_spend__monthly_retailer__sum,
        null as t012__refund__monthly_retailer__dcount,
        null as t022__aov_inc_refunds__monthly_retailer__avg,
        null as t023__arpu_inc_refunds__monthly_retailer__avg,
        null as t024__atf_inc_refunds__monthly_retailer__avg,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage,
        null as t017__duplicate_txn__monthly_retailer__dcount
    from lc_metric
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u112_active_users_inc_refunds__retailer_monthly__dcount_uid,
        null as u111_active_users_inc_refunds__retailer_monthly__cdcount_uid,
        t025__txns_and_refunds__monthly_retailer__dcount,
        t020__net_spend__monthly_retailer__sum,
        t012__refund__monthly_retailer__dcount,
        null as t022__aov_inc_refunds__monthly_retailer__avg,
        null as t023__arpu_inc_refunds__monthly_retailer__avg,
        null as t024__atf_inc_refunds__monthly_retailer__avg,
        t019__duplicate_txn_per_txn__monthly_retailer__percentage,
        t017__duplicate_txn__monthly_retailer__dcount
    from txn_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u112_active_users_inc_refunds__retailer_monthly__dcount_uid,
        null as u111_active_users_inc_refunds__retailer_monthly__cdcount_uid,
        null as t025__txns_and_refunds__monthly_retailer__dcount,
        null as t020__net_spend__monthly_retailer__sum,
        null as t012__refund__monthly_retailer__dcount,
        t022__aov_inc_refunds__monthly_retailer__avg,
        t023__arpu_inc_refunds__monthly_retailer__avg,
        t024__atf_inc_refunds__monthly_retailer__avg,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage,
        null as t017__duplicate_txn__monthly_retailer__dcount
    from txn_avg
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        u112_active_users_inc_refunds__retailer_monthly__dcount_uid,
        u111_active_users_inc_refunds__retailer_monthly__cdcount_uid,
        null as t025__txns_and_refunds__monthly_retailer__dcount,
        null as t020__net_spend__monthly_retailer__sum,
        null as t012__refund__monthly_retailer__dcount,
        null as t022__aov_inc_refunds__monthly_retailer__avg,
        null as t023__arpu_inc_refunds__monthly_retailer__avg,
        null as t024__atf_inc_refunds__monthly_retailer__avg,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage,
        null as t017__duplicate_txn__monthly_retailer__dcount
    from user_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u112_active_users_inc_refunds__retailer_monthly__dcount_uid,
        null as u111_active_users_inc_refunds__retailer_monthly__cdcount_uid,
        null as t025__txns_and_refunds__monthly_retailer__dcount,
        null as t020__net_spend__monthly_retailer__sum,
        null as t012__refund__monthly_retailer__dcount,
        null as t022__aov_inc_refunds__monthly_retailer__avg,
        null as t023__arpu_inc_refunds__monthly_retailer__avg,
        null as t024__atf_inc_refunds__monthly_retailer__avg,
        null as t019__duplicate_txn_per_txn__monthly_retailer__percentage,
        null as t017__duplicate_txn__monthly_retailer__dcount
    from pll_metrics
)

select *
from combine_all
