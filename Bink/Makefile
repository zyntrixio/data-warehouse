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