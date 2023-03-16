from prefect import flow, task
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector

@task(name = "get-dbt-profile")
def get_dbt_cli_profile(env):
    dbt_connector = SnowflakeConnector(
        schema="BINK",
        database={"dev": "DEV", "prod": "BINK"}[env],
        warehouse="ENGINEERING",
        credentials=SnowflakeCredentials.load("snowflake-transform-user"),
    )
    dbt_cli_profile = DbtCliProfile(
        name="Bink",
        target="target",
        target_configs=SnowflakeTargetConfigs(connector=dbt_connector),
    )
    return dbt_cli_profile

def dbt_cli_task(dbt_cli_profile, command):
    return trigger_dbt_cli_command(
        command=command,
        overwrite_profiles=True,
        profiles_dir=f"/opt/github.com/binkhq/data-warehouse/Prefect",
        project_dir=f"/opt/github.com/binkhq/data-warehouse/Bink",
        dbt_cli_profile=dbt_cli_profile,
    )

@flow(name="Viator_weekly")
def run(env: str):
    dbt_cli_profile = get_dbt_cli_profile(env, "get-dbt-profile")
    dbt_cli_task(dbt_cli_profile, "dbt run-operation send_viator_files", "viator-file-delivery")
