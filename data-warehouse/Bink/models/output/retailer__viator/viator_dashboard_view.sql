/*
Created by:         Christopher Mitchell
Created date:       2023-07-05
Last modified by:   Christopher Mitchell
Last modified date: 2024-01-15

Description:
    Datasource to produce tableau dashboard for Viator
Parameters:
    source_object       - lc__links_joins__monthly_retailer
                        - trans__trans__monthly_retailer
                        - trans__avg__monthly_retailer
                        - user__transactions__monthly_retailer
                        - voucher__counts__monthly_retailer
*/

with lc_metric as (
    select
        *,
        'JOINS' as category
    from {{ ref('lc__links_joins__monthly_retailer') }}
    where loyalty_plan_company = 'Viator'
),

txn_metrics as (
    select
        *,
        'SPEND' as category
    from {{ ref('trans__trans__monthly_retailer') }}
    where loyalty_plan_company = 'Viator'
),

txn_avg as (
    select
        *,
        'SPEND' as category
    from {{ ref('trans__avg__monthly_retailer') }}
    where loyalty_plan_company = 'Viator'
),

user_metrics as (
    select
        *,
        'USERS' as category
    from {{ ref('user__transactions__monthly_retailer') }}
    where loyalty_plan_company = 'Viator'
),

pll_metrics as (
    select
        *,
        'JOINS' as category
    from {{ ref('lc__pll__monthly_retailer') }}
    where loyalty_plan_company = 'Viator'
),

voucher_metrics as (
    select
        *,
        'VOUCHERS' as category
    from {{ ref('voucher__counts__monthly_retailer') }}
    where loyalty_plan_company = 'Viator'
),

combine_all as (
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        lc347__successful_loyalty_card_joins__monthly_retailer__count,
        lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        null as t014__aov__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t012__refund__monthly_retailer__dcount,
        null as t011__txns__monthly_retailer__dcount,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as t009__spend__monthly_retailer__sum,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        null as v013__redeemed_vouchers__monthly_retailer__dcount,
        null as v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    from lc_metric
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        null as t014__aov__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        t012__refund__monthly_retailer__dcount,
        t011__txns__monthly_retailer__dcount,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        t009__spend__monthly_retailer__sum,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        null as v013__redeemed_vouchers__monthly_retailer__dcount,
        null as v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    from txn_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        t014__aov__monthly_retailer__avg,
        t016__atf__monthly_retailer__avg,
        t015__arpu__monthly_retailer__avg,
        null as t012__refund__monthly_retailer__dcount,
        null as t011__txns__monthly_retailer__dcount,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as t009__spend__monthly_retailer__sum,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        null as v013__redeemed_vouchers__monthly_retailer__dcount,
        null as v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    from txn_avg
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        null as t014__aov__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t012__refund__monthly_retailer__dcount,
        null as t011__txns__monthly_retailer__dcount,
        u107_active_users__retailer_monthly__dcount_uid,
        u108_active_users_retailer_monthly__cdcount_uid,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as t009__spend__monthly_retailer__sum,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        null as v013__redeemed_vouchers__monthly_retailer__dcount,
        null as v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    from user_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        null as t014__aov__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t012__refund__monthly_retailer__dcount,
        null as t011__txns__monthly_retailer__dcount,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as t009__spend__monthly_retailer__sum,
        null as v012__issued_vouchers__monthly_retailer__dcount,
        null as v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        null as v013__redeemed_vouchers__monthly_retailer__dcount,
        null as v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    from pll_metrics
    union all
    select
        date,
        category,
        loyalty_plan_name,
        loyalty_plan_company,
        null as lc379__successful_loyalty_card_joins__monthly_retailer__csum,
        null as lc347__successful_loyalty_card_joins__monthly_retailer__count,
        null
            as lc333__successful_loyalty_card_join_mrkt_opt_in__monthly_retailer__count,
        null as t014__aov__monthly_retailer__avg,
        null as t016__atf__monthly_retailer__avg,
        null as t015__arpu__monthly_retailer__avg,
        null as t012__refund__monthly_retailer__dcount,
        null as t011__txns__monthly_retailer__dcount,
        null as u107_active_users__retailer_monthly__dcount_uid,
        null as u108_active_users_retailer_monthly__cdcount_uid,
        null as lc201__loyalty_card_active_pll__monthly_retailer__pit,
        null as t009__spend__monthly_retailer__sum,
        v012__issued_vouchers__monthly_retailer__dcount,
        v009__issued_vouchers__monthly_retailer__cdsum_voucher,
        v013__redeemed_vouchers__monthly_retailer__dcount,
        v010__redeemed_vouchers__monthly_retailer__cdsum_voucher
    from voucher_metrics
)

select *
from combine_all
