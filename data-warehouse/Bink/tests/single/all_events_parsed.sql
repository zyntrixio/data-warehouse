{{
    config(
        tags=["business"],
        meta={
            "description": "Check if all events generated in raw make there way to a fact table in prod.",
            "test_type": "Business",
        },
    )
}}

with
    events_src as (
        select event_id
        from {{ ref("stg_hermes__EVENTS") }}
        where
            event_type in (
                'lc.addandauth.request',
                'lc.auth.request',
                'lc.auth.failed',
                'lc.statuschange',
                'lc.join.request',
                'lc.join.success',
                'lc.register.failed',
                'lc.addandauth.success',
                'lc.join.failed',
                'lc.register.success',
                'lc.auth.success',
                'lc.removed',
                'lc.addandauth.failed',
                'lc.register.request',
                'payment.account.added',
                'payment.account.status.change',
                'payment.account.removed',
                'transaction.exported',
                'transaction.duplicate',
                'user.created',
                'user.session.start',
                'user.deleted'
            -- ,'pll_link.statuschange'
            -- ,'transaction.exported.response'
            )
    ),
    fact_tables as (
        {% set get_tables %}
    SELECT TABLE_NAME
    FROM PROD.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{{target.schema}}'
    AND TABLE_NAME LIKE 'FACT%'
        {% endset %}

        {# Get column list #}
        {% set results = run_query(get_tables) %}

        {% if execute %}
        {# Return the first column as a list #}
        {% set results_list = results.columns[0].values() %}
        {% else %} {% set results_list = [] %}
        {% endif %}

        {%- for item in results_list -%}
        {%- if item not in ("FACT_VOUCHER", "FACT_PLL_LINK_STATUS_CHANGE") %}
        {%- if not loop.first %}
        union all
        {% endif %}
        select event_id
        from prod.{{ target.schema }}.{{ item }}
        {% endif %}
        {%- endfor -%}
    ),
    events_minus_facts as (
        select event_id
        from events_src
        except
        select event_id
        from fact_tables
    ),
    facts_minus_events as (
        select event_id
        from fact_tables
        except
        select event_id
        from events_src
    ),
    sum_except_all as (
        select *
        from events_minus_facts
        union all
        select *
        from facts_minus_events
    )

select *
from sum_except_all
