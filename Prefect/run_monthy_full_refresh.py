import os

from prefect import Flow, task
from prefect.client.secrets import Secret
from prefect.run_configs import DockerRun
from prefect.schedules import Schedule, clocks
from prefect.storage import Docker
from prefect.tasks.dbt import DbtShellTask

DBT_DIRECTORY = "Bink"
DBT_ENV = os.getenv("dbt_environment")
DBT_PROFILE = "Bink"
SNOWFLAKE_ACCOUNT = Secret("bink_snowflake_account").get()
SNOWFLAKE_PASSWORD = Secret("bink_snowflake_password").get()


@task(name="Snowflake connection")
def snowflake_connection():
    filein = "profiles_temp.yml"
    fileout = "/dbt/profiles.yml"

    with open(filein, "r") as f:
        filedata = f.read()

    newdata = filedata.replace("p_snowflake_account", SNOWFLAKE_ACCOUNT).replace(
        "p_snowflake_password", SNOWFLAKE_PASSWORD
    )

    with open(fileout, "w") as f:
        f.write(newdata)


def make_dbt_task(command, name):
    return DbtShellTask(
        command=command,
        name=name,
        profiles_dir=".",
        profile_name=DBT_PROFILE,
        environment=DBT_ENV,
        helper_script="cd dbt",  ##  refers to the dbt dir within the docker image
        return_all=True,
        log_stderr=True,
    )


dbt_deps_task = make_dbt_task("dbt deps", "DBT Dependencies")
dbt_run_task = make_dbt_task(f"dbt run --full-refresh -t {DBT_ENV}", "DBT Run")
dbt_outp_test_task = make_dbt_task(f'dbt test --exclude tag:"source" tag:"business"  -t {DBT_ENV}', "DBT Output Tests")
dbt_business_test_task = make_dbt_task(f'dbt test --select tag:"business"  -t {DBT_ENV}', "DBT Business Tests")

docker_storage = Docker(
    image_name="bink_elt_full_load",
    files={  ## dictionary of local-path:docker-image-path items
        f"{os.getcwd()}/../{DBT_DIRECTORY}": "/dbt",
        f"{os.getcwd()}/profiles_temp.yml": "./profiles_temp.yml",
    },
    python_dependencies=["dbt-snowflake"],  ## List all pip packages here
)

schedule = Schedule(clocks=[clocks.CronClock("0 5 * * sun#1")])  ## Runs at 5:00 first Sunday of the month

with Flow(name="Bink ELT Monthly Full Load", run_config=DockerRun(), storage=docker_storage, schedule=schedule) as flow:

    compile_profiles_temp = snowflake_connection()

    dbt_deps = dbt_deps_task(upstream_tasks=[compile_profiles_temp])

    dbt_run = dbt_run_task(upstream_tasks=[dbt_deps])

    dbt_outp_test = dbt_outp_test_task(upstream_tasks=[dbt_run])

    dbt_business_test = dbt_business_test_task(upstream_tasks=[dbt_outp_test])
