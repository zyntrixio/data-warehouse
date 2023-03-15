build-dev:
	prefect deployment build ./run_elt.py:run --name dev -q prod-agent --tag dev-elt --output dev-deployment.yaml
	prefect deployment build ./run_monthly_full_refresh.py:run --name dev -q prod-agent --tag dev-elt --output dev-full-refresh-deployment.yaml
deploy-dev: build-dev
	prefect deployment apply dev-deployment.yaml
	prefect deployment apply dev-full-refresh-deployment.yaml
build-prod:
	prefect deployment build ./run_elt.py:run --name prod -q prod-agent --tag prod-elt --output prod-deployment.yaml
	prefect deployment build ./run_monthly_full_refresh.py:run --name prod -q prod-agent --tag prod-elt --output prod-full-refresh-deployment.yaml
deploy-prod: build-prod
	prefect deployment apply prod-deployment.yaml
	prefect deployment apply prod-full-refresh-deployment.yaml
run-dev: deploy
	prefect deployment run ELT_Flow/dev
	prefect deployment run Full_Refresh_Flow/dev
build-viator:
	prefect deployment build ./viator_weekly.py:run --name prod -q prod-agent --tag viator-files --output viator-deployment.yaml
deploy-viator:
	prefect deployment apply viator-deployment.yaml
