/*
Created by:         Anand Bhakta
Created date:       2023-06-27
Last modified by:   
Last modified date: 

Description:
    Datasource to produce lloyds mi dashboard - loyalty_cards_error_funnel
Parameters:
    source_object       - LC201__LOYALTY_CARD_JOURNEY_FUNNEL__USER_LEVEL__UID
*/
with
    funnel as (
        select *
        from {{ ref("LC201__LOYALTY_CARD_JOURNEY_FUNNEL__USER_LEVEL__UID") }}
        where channel = 'LLOYDS'
    ),
    model as (select * from {{ ref("src__lookup_sankey_model") }}),
    combine as (
        select f.*, m.* exclude link from funnel f inner join model m on m.link = f.link
    )

select *
from combine
