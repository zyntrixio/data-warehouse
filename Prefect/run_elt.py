from prefect import Flow, task
from prefect.tasks.dbt import DbtShellTask
from prefect.tasks.airbyte.airbyte import AirbyteConnectionTask
from prefect.run_configs import DockerRun
from prefect.storage import Docker
from prefect.schedules import Schedule, clocks
import os

# run_airbyte = AirbyteConnectionTask(
#     connection_id='0e3e8d90-15c5-4a23-a218-a0e5e5ddd1d8'
# )

DBT_DIRECTORY = 'Bink'
DBT_PROFILE = 'Bink'

def make_dbt_task(command, name):
    return DbtShellTask(
        command=command
        ,name=name
        ,profiles_dir='.'
        ,profile_name=DBT_PROFILE
        ,environment='dev'
        ,helper_script='cd dbt' ##  refers to the dbt dir within the docker image
        ,return_all=True
        ,log_stderr=True
    )

dbt_run_task = make_dbt_task('dbt run', 'Run')

dbt_test_task = make_dbt_task('dbt test', 'Test')

docker_storage = Docker(
    image_name="box_elt_flow_image"
    ,files={ ## dictionary of local-path:docker-image-path items
        f'{os.getcwd()}/../{DBT_DIRECTORY}':'/dbt'
        ,f'{os.getcwd()}/profiles.yml':'/dbt/profiles.yml'
    }
    ,python_dependencies=['dbt-snowflake'] ## List all pip packages here
    )

# schedule = Schedule(clocks=[clocks.CronClock("0 7 * * *")]) ## Runs at 7:00 every day

with Flow(
        name="Bink ELT"
        ,run_config=DockerRun()
        ,storage=docker_storage
        ,schedule=schedule
        ) as flow:

        dbt_deps = make_dbt_task('dbt deps', 'Dependencies')

        dbt_run = dbt_run_task(
            upstream_tasks=[dbt_deps]
        )

        dbt_test = dbt_test_task(
            upstream_tasks=[dbt_run]
        )