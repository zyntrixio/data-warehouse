from prefect import flow, task
from prefect.blocks.system import String
from prefect_dask.task_runners import DaskTaskRunner
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector
from datetime import datetime


date = datetime.today().strftime("%Y%m%d")
file_name = f"viator_weekly_transactions_{date}"
create_table_sql = f"""CREATE OR REPLACE TABLE output.viator_weekly_files.{file_name} (AMOUNT NUMBER(38,2), TRANSACTION_DATE DATETIME, AUTH VARCHAR(50), MID VARCHAR(38), LAST_FOUR VARCHAR(4))
                                    AS (
                                    SELECT
                                                t.SPEND_AMOUNT AS AMOUNT
                                                ,t.TRANSACTION_DATE AS TRANSACTION_DATE
                                                ,t.AUTH_CODE AS AUTH
                                                ,t.MERCHANT_ID AS MID
                                                ,p.PAN_END AS LAST_FOUR
                                            FROM
                                                "PROD"."BINK"."FACT_TRANSACTION" t
                                            LEFT JOIN
                                                "PROD"."BINK"."DIM_PAYMENT_ACCOUNT" p
                                                    ON t.PAYMENT_ACCOUNT_ID = p.PAYMENT_ACCOUNT_ID
                                            WHERE
                                                PROVIDER_SLUG = 'bpl-viator'
                                                AND EVENT_DATE_TIME >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7 DAY'
                                                AND EVENT_DATE_TIME < DATE_TRUNC('week', CURRENT_DATE)
                                    );"""
                                    
copy_into_sql = f"""COPY INTO 'azure://uksouthprod89hg.blob.core.windows.net/viator/{file_name}.csv'
                            FROM output.viator_weekly_files.{file_name}
                            file_format=(format_name=OUTPUT.VIATOR_WEEKLY_FILES.CSV_LOADER compression = NONE) OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE
                            storage_integration = viator_azure;"""

@task
def setup_table(block_name: str) -> None:
    with SnowflakeConnector.load(block_name) as connector:
        connector.execute(
            create_table_sql
        )

@task
def copy_to_azure(block_name: str) -> None:
    with SnowflakeConnector.load(block_name) as connector:
        connector.execute(
            copy_into_sql
        )

@flow
def viator_weekly_file_delivery(block_name: str) -> None:
    setup_table(block_name)
    copy_to_azure(block_name)

viator_weekly_file_delivery()
