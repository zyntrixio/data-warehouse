register-dev:
	prefect create project Bink-dev
	dbt_environment=dev prefect register --project Bink-dev -l dev -p run_elt.py --no-schedule

register-prod:
	prefect create project Bink
	dbt_environment=prod prefect register --project Bink -l prod -p run_elt.py

docker-agent-start:
	prefect agent docker start -n test-docker-agent -l dev