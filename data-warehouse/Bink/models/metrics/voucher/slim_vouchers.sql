/*
Created by:         Christopher Mitchell
Created date:       2023-07-04
Last modified by:   Christopher Mitchell
Last modified date: 2023-08-24

Description:
    Count of Voucher States daily by channel brand and retailer
Parameters:
    source_object       - voucher_trans
                        - dim_date
*/
{% macro award_vouchers() %}
    {% set vouchers = {} %}
    {% for txn in txns_trans %}
        {% if txn.amount > 7.50 %}
            {% set user_id = txn.user_id %}
            {% set txn_date = txn.date.date() %}
            {% set last_voucher_date = vouchers.get(user_id) %}
            {% if not last_voucher_date or (txn_date - last_voucher_date).days >= 1 %}
                {% set vouchers = vouchers.update({user_id: txn_date}) %}
                INSERT INTO vouchers (user_id, amount, date)
                VALUES ({{ user_id }}, 5.00, '{{ txn_date }}');
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
