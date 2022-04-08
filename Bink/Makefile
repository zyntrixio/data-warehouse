clean:
	dbt clean

compile: clean
	dbt compile

deps:
	dbt deps

run: deps
	dbt run

test: compile deps
	dbt test

source_test: compile deps
	dbt test --select tag:source
	