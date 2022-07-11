from prefect import Flow, task
from prefect.tasks.dbt import DbtShellTask
from prefect.tasks.airbyte.airbyte import AirbyteConnectionTask
from prefect.run_configs import DockerRun
from prefect.storage import Docker
from prefect.schedules import Schedule, clocks
from prefect.client.secrets import Secret
import os

DBT_DIRECTORY = 'Bink'
DBT_PROFILE = 'Bink'
DBT_ENV = os.getenv('dbt_environment')
AIRBYTE_EVENTS_CONNECTION_ID='62d2288c-11b2-4a5c-bbc1-4f0db35a9a93'
AIRBYTE_HERMES_CONNECTION_ID='aa27ccee-6641-4de6-982a-37daf0700c16'
AIRBYTE_IP=Secret("bink_airbyte_ip").get()
SNOWFLAKE_ACCOUNT=Secret("bink_snowflake_account").get()
SNOWFLAKE_PASSWORD=Secret("bink_snowflake_password").get()


@task(name='Snowflake connection')
def snowflake_connection():
    filein = 'profiles_temp.yml'
    fileout = '/dbt/profiles.yml'

    with open(filein,'r') as f:
        filedata = f.read()

    newdata = filedata.replace("p_snowflake_account",SNOWFLAKE_ACCOUNT).replace('p_snowflake_password', SNOWFLAKE_PASSWORD)

    with open(fileout,'w') as f:
        f.write(newdata)
    

def make_airbyte_task(name, connection_id):
    return AirbyteConnectionTask(
            airbyte_server_host=AIRBYTE_IP
            ,connection_id=connection_id
            ,name=name
        )


def make_dbt_task(command, name):
    return DbtShellTask(
        command=command
        ,name=name
        ,profiles_dir='.'
        ,profile_name=DBT_PROFILE
        ,environment=DBT_ENV
        ,helper_script='cd dbt' ##  refers to the dbt dir within the docker image
        ,return_all=True
        ,log_stderr=True
    )

dbt_deps_task = make_dbt_task('dbt deps', 'DBT Dependencies')
dbt_run_task = make_dbt_task(f'dbt run --target: {DBT_ENV}', 'DBT Run')
dbt_src_test_task = make_dbt_task('dbt test --select tag:source', 'DBT Source Tests')
dbt_outp_test_task = make_dbt_task('dbt test --exclude tag:source', 'DBT Output Tests')

docker_storage = Docker(
    image_name="box_elt_flow_image"
    ,files={ ## dictionary of local-path:docker-image-path items
        f'{os.getcwd()}/../{DBT_DIRECTORY}':'/dbt'
        ,f'{os.getcwd()}/profiles_temp.yml':'./profiles_temp.yml'
    }
    ,python_dependencies=['dbt-snowflake'] ## List all pip packages here
    )

schedule = Schedule(clocks=[clocks.CronClock("0 6 * * *")]) ## Runs at 7:00 every day

with Flow(
        name="Bink ELT"
        ,run_config=DockerRun()
        ,storage=docker_storage
        ,schedule=schedule
        ) as flow:

        compile_profiles_temp = snowflake_connection()

        airbyte_sync_events = make_airbyte_task('Sync Events',AIRBYTE_EVENTS_CONNECTION_ID)

        # airbyte_sync_hermes = make_airbyte_task('Sync Hermes',AIRBYTE_HERMES_CONNECTION_ID)

        dbt_deps = dbt_deps_task(
            upstream_tasks=[
                airbyte_sync_events
                # ,airbyte_sync_hermes
                ,compile_profiles_temp
                ]
        )

        dbt_src_test = dbt_src_test_task(
            upstream_tasks=[dbt_deps]
        )

        dbt_run = dbt_run_task(
            upstream_tasks=[dbt_src_test]
        )

        dbt_outp_test = dbt_outp_test_task(
            upstream_tasks=[dbt_run]
        )