# Data Warehouse

Data Warehouse repostiory containing DBT models and Prefect Orchestration.

LOTS OF TECHNICAL DEBT, PLEASE BE CAREFUL.

## Installation

```shell
git clone {url/ssh}
cd data-warehouse && cd bink
poetry shell
poetry install
dbt deps
```
And you are ready to go.

## Usage

```shell
dbt run -t uat # for UAT
dbt run -t prod # for prod
dbt run -t --select {name of model} # for a specific model
```

## Contributing

We operate on a two branch system with development for features on the development branch and indidivual features as separate branches.
Pull requests must be raised. Failure to do so will lead to jokes at your expense.