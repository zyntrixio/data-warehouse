clean:
	dbt clean

compile: clean
	dbt compile

deps:
	dbt deps

run: compile deps
	dbt run

test: compile deps
	dbt test

source_test: compile deps
	dbt test --select tag:source
	
docs:
	dbt docs generate
	dbt docs serve