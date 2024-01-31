from prefect import flow, task
from prefect.blocks.system import String
from prefect_dask.task_runners import DaskTaskRunner
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector
from prefect_azure.credentials import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_upload

from datetime import datetime
import pandas as pd


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

create_table_sql_dev = f"""CREATE OR REPLACE TABLE output.viator_weekly_files.{file_name} (AMOUNT NUMBER(38,2), TRANSACTION_DATE DATETIME, AUTH VARCHAR(50), MID VARCHAR(38), LAST_FOUR VARCHAR(4))
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
                                    );"""
                                    
copy_into_sql = f"""COPY INTO 'azure://uksouthprod89hg.blob.core.windows.net/viator/{file_name}.csv'
                            FROM output.viator_weekly_files.{file_name}
                            file_format=(format_name=OUTPUT.VIATOR_WEEKLY_FILES.CSV_LOADER compression = NONE) OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE
                            storage_integration = viator_azure;"""

fetch_data = f"SELECT * FROM output.viator_weekly_files.{file_name}"

@task
def setup_table(block_name: str) -> None:
    with SnowflakeConnector.load(block_name) as connector:
        connector.execute(
            create_table_sql_dev
        )

@task
def collect_data(block_name: str) -> pd.DataFrame:
    with SnowflakeConnector.load(block_name) as connector:
        with connector.get_connection() as connection:
            with connection.cursor() as con:
                con.execute(fetch_data)
                results = con.fetchall()
                df = pd.DataFrame(results, columns=[col[0] for col in con.description])
                # print(df)
    return df

@flow
def upload_to_blob(data: pd.DataFrame, file_name: str) -> None:
    connection_string = String.load("dp-staging-blob").value
    blob_storage_credentials = AzureBlobStorageCredentials(connection_string=connection_string)
    blob_name = f"{file_name}.csv"
    data_csv = data.to_csv(index=False)
    print(data)
    print(data.info())

    blob = blob_storage_upload(
        data=data_csv.encode(),
        container="dp-staging-blob",
        blob=blob_name,
        blob_storage_credentials=blob_storage_credentials,
        overwrite=False,
    )
    return blob



@flow
def viator_weekly_file_delivery(block_name: str = "snowflake-transform-user",) -> None:
    setup_table(block_name)
    data = collect_data(block_name)
    upload_to_blob(data, file_name)

viator_weekly_file_delivery()
