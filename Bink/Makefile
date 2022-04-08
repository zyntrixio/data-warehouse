clean:
	dbt clean

compile: clean
	dbt compile

deps: clean
	dbt deps

run: compile deps
	dbt run

test: compile
	dbt test

source_test: compile
	dbt test --select tag:source
	