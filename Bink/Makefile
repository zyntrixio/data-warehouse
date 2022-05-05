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
	dbt run
	dbt test

test: compile deps
	dbt test

source_test: compile deps
	dbt test --select tag:source

output_tests: compile deps
	dbt test --exclude tag:source
	
docs:
	dbt docs generate
	dbt docs serve