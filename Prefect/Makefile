register-dev:
	prefect create project Bink-dev
	export dbt_environment="dev"
	prefect register --project Bink-dev -l dev -p run_elt.py --no-schedule
	prefect register --project Bink-dev -l dev -p run_monthy_full_refresh.py --no-schedule

register-prod:
	prefect create project Bink
	export dbt_environment="prod"
	prefect register --project Bink -l prod -p run_elt.py
	prefect register --project Bink -l prod -p run_monthy_full_refresh.py

docker-agent-start:
	prefect agent docker start -n test-docker-agent -l dev