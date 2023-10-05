FROM ghcr.io/binkhq/python:3.11-poetry as build
ADD data-warehouse /build/data-warehouse
WORKDIR /build/data-warehouse/Bink
RUN poetry export --without-hashes --format=requirements.txt > requirements.txt

FROM ghcr.io/binkhq/python:3.11
WORKDIR /app
COPY --from=build /build /app/
RUN pip install -r /app/data-warehouse/Bink/requirements.txt && \
    dbt deps --project-dir /app/data-warehouse/Bink
RUN apt-get update && \
    apt-get -y --no-install-recommends install make && \
    apt-get clean
