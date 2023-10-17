from prefect import flow, task
from prefect.blocks.system import Secret, String
from prefect_airbyte.connections import trigger_sync
from prefect_dask.task_runners import DaskTaskRunner
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector


@task
def get_dbt_cli_profile(env):
    dbt_connector = SnowflakeConnector(
        schema="BINK",
        database="SANDBOX",
        warehouse="ENGINEERING",
        credentials=SnowflakeCredentials.load("snowflake-transform-user"),
    )
    dbt_cli_profile = DbtCliProfile(
        name="Bink_New",
        target="target",
        target_configs=SnowflakeTargetConfigs(connector=dbt_connector),
    )
    return dbt_cli_profile


def dbt_cli_task(dbt_cli_profile, command):
    return trigger_dbt_cli_command(
        command=command,
        overwrite_profiles=True,
        profiles_dir="/app/data-warehouse/Prefect",
        project_dir="/app/data-warehouse/Bink",
        dbt_cli_profile=dbt_cli_profile,
    )


@flow(name="ELT_Extractions", task_runner=DaskTaskRunner)
def trigger_extractions():
    copybot_output = trigger_sync.submit(
        airbyte_server_host=String.load("airbyte-ip").value,
        connection_id=String.load("airbyte-snowstorm-connection").value,
        poll_interval_s=3,
        status_updates=True,
    )
    trigger_sync.submit(
        airbyte_server_host=String.load("airbyte-ip").value,
        connection_id=String.load("airbyte-hermes-connection").value,
        poll_interval_s=3,
        status_updates=True,
        wait_for=[copybot_output],
    )
    trigger_sync.submit(
        airbyte_server_host=String.load("airbyte-ip").value,
        connection_id=String.load("airbyte-harmonia-connection").value,
        poll_interval_s=3,
        status_updates=True,
        wait_for=[copybot_output],
    )


@flow(name="ELT_Flow")
def run(
    env: str,
    is_trigger_extractions: bool = True,
    is_run_source_tests: bool = True,
    is_run_transformations: bool = True,
    is_run_output_tests: bool = True,
    is_grant_share: bool = True,
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
    if is_grant_share:
        dbt_cli_task(dbt_cli_profile, f'dbt run-operation uat_grants --args "env: {env}"')
