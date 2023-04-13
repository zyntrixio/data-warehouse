FROM ghcr.io/binkhq/python:3.11

RUN pip install \
            adlfs \
            dbt-snowflake \
            prefect-dbt \
            prefect-snowflake \
            prefect-airbyte \
            prefect-dask \
            prefect \
            prefect-shell

WORKDIR /app
ADD data-warehouse /app/data-warehouse
ADD data-warehouse-dashboards /app/data-warehouse-dashboards
