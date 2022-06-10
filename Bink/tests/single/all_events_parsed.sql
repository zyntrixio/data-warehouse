with events_src as (
    SELECT EVENT_ID
    FROM {{ ref('stg_hermes__EVENTS')}}
)

 ,fact_tables as (
    {%- for item in [
        ref('fact_loyalty_card_add_auth')
        ,ref('fact_loyalty_card_auth')
        ,ref('fact_loyalty_card_join')
        ,ref('fact_loyalty_card_register')
        ,ref('fact_loyalty_card_removed')
        ,ref('fact_loyalty_card_status_change')
        ,ref('fact_payment_account_status_change')
        ,ref('fact_payment_account')
        ,ref('fact_transaction')
        ,ref('fact_user')
        ]
    -%}
    {%- if not loop.first %} UNION ALL {% endif %}
    SELECT event_id FROM {{ item }}
    {%- endfor -%}
)

,events_minus_facts as (
    SELECT EVENT_ID FROM events_src
    EXCEPT 
    SELECT EVENT_ID FROM fact_tables
)

,facts_minus_events as (
    SELECT EVENT_ID FROM fact_tables
    EXCEPT
    SELECT EVENT_ID FROM events_src
)

,sum_except_all as (
    SELECT * FROM events_minus_facts
    UNION ALL
    SELECT * FROM facts_minus_events
)

SELECT * FROM sum_except_all