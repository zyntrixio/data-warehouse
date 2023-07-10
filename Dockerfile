FROM ghcr.io/binkhq/python:3.11

RUN apt-get update && apt-get -y install make && \
    rm -rf /var/lib/apt/lists/*

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
