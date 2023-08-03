# Data Warehouse

Data Warehouse repostiory containing DBT models and Prefect Orchestration.

LOTS OF TECHNICAL DEBT, PLEASE BE CAREFUL.

## Installation

Firstly, any users of this repository will require DBT to be installed locally. Please follow the link below:

[DBT Installation Instructions](https://docs.getdbt.com/docs/core/homebrew-install)

You will then need a copy of the company ```profiles.yaml``` to provide you with the correct data warehouse locations, environments, and passwords.

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

# to do dockerfile guide to host prefect locally for development
```

## Contributing

We operate on a two branch system with development for features on the development branch and indidivual features as separate branches.
Pull requests must be raised. Failure to do so will lead to jokes at your expense.