clean:
	dbt clean

compile: clean
	dbt compile --exclude +elementary

deps:
	dbt deps

run: compile
	dbt run --exclude +elementary

run_table:
	dbt compile --select $(table)
	dbt run --select $(table)

run_full: clean
	dbt compile
	dbt deps
	dbt run --full-refresh
	dbt test

test: compile deps
	dbt test

test_source: compile
	dbt test --select tag:source

test_output: compile
	dbt test --exclude tag:"source" tag:"business"

test_business: compile
	dbt test --select tag:"business"
	
docs:
	dbt docs generate
	dbt docs serve