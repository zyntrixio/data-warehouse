{#
Created by:         Anand Bhakta
Created date:       2023-02-23
Last modified by:   
Last modified date: 

Description:
    Operation to create a table with todays date and send transactions for the last week from monday - sunday

Parameters:
    source_object      - BINK.BINK.FACT_TRANSACTION
    #}

{% macro send_viator_files() %}

  {% set date_now = run_started_at.strftime("%Y%m%d")%}
  {% set file_path = ['azure://binkuksouthtableau.blob.core.windows.net/viator/viator_weekly_transactions_',date_now,'.csv']|join%}

  {% set create_table %}
      CREATE OR REPLACE TABLE bink.viator_weekly_files.viator_weekly_transactions_{{date_now}} (AMOUNT NUMBER(38,2), TRANSACTION_DATE DATETIME, AUTH NUMBER(38,0), MID NUMBER(38,0), LAST_FOUR NUMBER(4,0))
                                      AS (
                                      SELECT
                                                  t.SPEND_AMOUNT AS AMOUNT
                                                  ,t.TRANSACTION_DATE AS TRANSACTION_DATE
                                                  ,t.AUTH_CODE AS AUTH
                                                  ,t.MERCHANT_ID AS MID
                                                  ,p.PAN_END AS LAST_FOUR
                                              FROM
                                                  BINK.BINK.FACT_TRANSACTION t
                                              LEFT JOIN
                                                  BINK.BINK.DIM_PAYMENT_ACCOUNT p
                                                      ON t.PAYMENT_ACCOUNT_ID = p.PAYMENT_ACCOUNT_ID
                                              WHERE
                                                  PROVIDER_SLUG = 'bpl-viator'
                                                  AND EVENT_DATE_TIME >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7 DAY'
                                                  AND EVENT_DATE_TIME < DATE_TRUNC('week', CURRENT_DATE)
                                      )
  {% endset %}
  {% set transfer_files %}
      COPY INTO '{{file_path}}'
                              FROM  bink.viator_weekly_files.viator_weekly_transactions_{{date_now}}
                              file_format=(format_name=BINK.BINK_STAGING.CSV_LOADER compression = NONE) OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE
                              storage_integration = viator_azure

  {% endset %}
  {{print(create_table)}}
  {{print(transfer_files)}}
  {% do run_query(create_table) %}
  {% do run_query(transfer_files) %}
  
{% endmacro %}
