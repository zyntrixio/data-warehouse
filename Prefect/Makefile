register:
	prefect create project Bink
	prefect register --project Bink -l azurerunner -p run_elt.py

docker-agent-start:
	prefect agent docker start -n azure-docker-agent -l azurerunner