/*
Created by:         Anand Bhakta
Created date:       2023-10-19
Last modified by:
Last modified date:

Description:
    Datasource to produce tableau dashboard for Slim Chickens
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
    where loyalty_plan_company = 'Slim Chickens'
),

txn_metrics as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__trans__monthly_retailer") }}
    where loyalty_plan_company = 'Slim Chickens'
),

txn_avg as (
    select
        *,
        'SPEND' as category
    from {{ ref("trans__avg__monthly_retailer") }}
    where loyalty_plan_company = 'Slim Chickens'
),

user_metrics as (
    select
        *,
        'USERS' as category
    from {{ ref("user__transactions__monthly_retailer") }}
    where loyalty_plan_company = 'Slim Chickens'
),

pll_metrics as (
    select
        *,
        'JOINS' as category
    from {{ ref("lc__pll__monthly_retailer") }}
    where loyalty_plan_company = 'Slim Chickens'
),

voucher_metrics as (
    select
        *,
        'VOUCHERS' as category
    from {{ ref("voucher__counts__monthly_retailer") }}
    where loyalty_plan_company = 'Slim Chickens'
),

combine_all as (
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        lc347__successful_loyalty_card_joins__monthly_retailer__count,
        lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as t011__txns__monthly_retailer__dcount,
        null as t012__refund__monthly_retailer__dcount,
        null as t010__refund__monthly_retailer__sum,
        null as t009__spend__monthly_retailer__sum,
        null as t014__aov__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher
    from lc_metric
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null
            as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        t011__txns__monthly_retailer__dcount,
        t012__refund__monthly_retailer__dcount,
        t010__refund__monthly_retailer__sum,
        t009__spend__monthly_retailer__sum,
        null as t014__aov__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher
    from txn_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null
            as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as t011__txns__monthly_retailer__dcount,
        null as t012__refund__monthly_retailer__dcount,
        null as t010__refund__monthly_retailer__sum,
        null as t009__spend__monthly_retailer__sum,
        t014__aov__monthly_retailer__avg,
        t015__arpu__monthly_retailer__avg,
        t016__atf__monthly_retailer__avg,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher
    from txn_avg
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null
            as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        u107_active_users__retailer_monthly__dcount_uid,
        u108_active_users_retailer_monthly__cdcount_uid,
        null as t011__txns__monthly_retailer__dcount,
        null as t012__refund__monthly_retailer__dcount,
        null as t010__refund__monthly_retailer__sum,
        null as t009__spend__monthly_retailer__sum,
        null as t014__aov__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher
    from user_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null
            as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as t011__txns__monthly_retailer__dcount,
        null as t012__refund__monthly_retailer__dcount,
        null as t010__refund__monthly_retailer__sum,
        null as t009__spend__monthly_retailer__sum,
        null as t014__aov__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher
    from pll_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null
            as lc351__successful_loyalty_card_links__monthly_retailer__dcount_user,
        null
            as lc334__successful_loyalty_card_join_mrkt_opt_in_per_successful_loyalty_card_join__monthly_retailer__percentage,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as t011__txns__monthly_retailer__dcount,
        null as t012__refund__monthly_retailer__dcount,
        null as t010__refund__monthly_retailer__sum,
        null as t009__spend__monthly_retailer__sum,
        null as t014__aov__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        v012__issued_vouchers__monthly_retailer__dcount,
        v009__issued_vouchers__monthly_retailer__cdsum_voucher
    from voucher_metrics
)

select *
from combine_all
