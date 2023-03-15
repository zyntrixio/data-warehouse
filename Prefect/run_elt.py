from prefect import flow, task
from prefect.blocks.system import Secret, String
from prefect_airbyte.connections import trigger_sync
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector


@task(name = "dbt-task")
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

@task(name="trigger-extraction", task_run_name="extracting-{connection}", retries=3, retry_delay_seconds=60)
def trigger_extraction_task(connection, wait_for):
        trigger_sync.submit(
            airbyte_server_host=String.load("airbyte-ip").value,
            connection_id=String.load(connection).value,
            poll_interval_s=3,
            status_updates=True,
            wait_for = wait_for
    )

@flow(name="ELT_Extractions")
def trigger_extractions():
    snowstorm = trigger_extraction_task("airbyte-snowstorm-connection", None )
    hermes = trigger_extraction_task("airbyte-hermes-connection", [snowstorm])
    trigger_extraction_task("airbyte-harmonia-connection", [hermes])


@flow(name="ELT_Flow")
def run(
    env: str,
    is_trigger_extractions: bool = True,
    is_run_source_tests: bool = True,
    is_run_transformations: bool = True,
    is_run_output_tests: bool = True,
):
    if is_trigger_extractions:
        trigger_extractions()
    dbt_cli_profile = get_dbt_cli_profile(env)
    dbt_cli_task(dbt_cli_profile, "dbt deps")
    if is_run_source_tests:
        dbt_cli_task(dbt_cli_profile, "dbt test --select tag:source")
    if is_run_transformations:
        dbt_cli_task(dbt_cli_profile, "dbt run")
    if is_run_output_tests:
        dbt_cli_task(dbt_cli_profile, 'dbt test --exclude tag:"source" tag:"business"')
        dbt_cli_task(dbt_cli_profile, "dbt test --select tag:business")
