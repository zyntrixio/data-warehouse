# Data Warehouse

Data Warehouse repostiory containing DBT models and Prefect Orchestration.

LOTS OF TECHNICAL DEBT, PLEASE BE CAREFUL.

## Installation

Firstly, any users of this repository will require DBT to be installed locally. Please follow the link below:

[DBT Installation Instructions](https://docs.getdbt.com/docs/core/pip-install)

You will then need a copy of the company `profiles.yaml` to provide you with the correct data warehouse locations, environments, and passwords.

Once this is installed, you can follow the commands below to get the repository up and running.

### DBT

```shell
git clone {url/ssh}
cd data-warehouse && cd bink
poetry shell
poetry install
dbt deps
```

And you are ready to go.

#### Usage

```shell
dbt run -t uat # for UAT
dbt run -t dev # for dev
dbt run -t uat -s {name of model} # for a specific model
dbt run -t dev -s /models/{Directory} # for all models in a directory
```

### Prefect Orchestration

```shell
git clone {url/ssh}
cd data-warehouse && cd prefect
poetry shell
poetry install

# to do dockerfile guide to host prefect locally for development
```

## Contributing

### DBT Data Testing

Proof of tests must be provided with Pull requests containing new models, seeds, sources, or macros.

This must show the output from the DBT Cli tool to show the tests.

You can run data testing via the DBT Cli command

```Shell
dbt test -t uat # for all tests
dbt test -t uat -s models/{directory} # for all tests on models in a directory
```

The output from these tests must be presented at code review to verify that data integrity is maintained.

### Linting and Formatting

#### Python Models

We use [Ruff](https://pypi.org/project/ruff/) to lint our python models, and [Black](https://pypi.org/project/black/) to format them.

They are installed as dev dependancies in our Poetry Venv for this project.

To run them please use the below commands:

```Shell
black /path/to/file
ruff check /path/to/file
```

This will format the model then run the linter over the model to check that it matches the rules, such as type safety, import clean up, etc.

#### DBT SQL Models

We use [sqlfluff](https://pypi.org/project/sqlfluff/) to Lint and Format our dbt sql models

This is installed as dev dependancies in our Poetry Venv for this project.

To run them please use the below commands:

```Shell
sqlfluff format -d snowflake -t jinja /path/to/file
sqlfluff lint -d snowflake -t jinja /path/to/file
```

This will format the query then run the linter over the model to check that it matches the rules, such as type safety, import clean up, etc.

### Pull Requests

New features, hotfixes, and changes, must be done via a new branch. This will then be merged into master via a Pull Request to ensure all required checks are performed.

Pull requests and code review must be performed before code can be merged into master branch.

Please link the Jira Story or Epic that the work is related to in the description.
