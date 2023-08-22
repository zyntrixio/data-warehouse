# Data Warehouse

Data Warehouse repostiory containing DBT models and Prefect Orchestration.

LOTS OF TECHNICAL DEBT, PLEASE BE CAREFUL.

## Installation

Firstly, any users of this repository will require DBT to be installed locally. Please follow the link below:

[DBT Installation Instructions](https://docs.getdbt.com/docs/core/homebrew-install)

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
dbt run -t prod # for prod
dbt run -t uat --select {name of model} # for a specific model
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
dbt test -t uat -s models/output # for all output layer models
```

The output from these tests must be presented at code review to verify that data integrity is maintained.

### Linting and Formatting

#### Python Models

We use Ruff to lint our python models, and Black to format them.

They are installed as dev dependancies in our Poetry Venv for this project.

To run them please use the below commands:

```Shell
black /path/to/file
ruff check /path/to/file
```

This will format the model then run the linter over the model to check that it matches the rules, such as type safety, import clean up, etc.

#### DBT SQL Models

We use sqlfluff to Lint and Format our dbt sql models

This is installed as dev dependancies in our Poetry Venv for this project.

To run them please use the below commands:

```Shell
sqlfluff format /path/to/file
sqlfluff lint /path/to/file
```

This will format the query then run the linter over the model to check that it matches the rules, such as type safety, import clean up, etc.

### Pull Requests

Pull requests and code review must be performed before code can be merged into master branch.

Please link the Jira Story or Epic that the work is related to in the description.
