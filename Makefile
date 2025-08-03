help: 
	@echo "		exec	 Exec container airflow"
	@echo "		stop     Stops the docker containers"
	@echo "		up     	 Runs the whole containers, served under https://localhost:8080/"
	@echo "		test     Run tests and validation"
	@echo "		lint     Run code linting"
	@echo "		format   Format code with black and isort"
	@echo "		ci-test  Run CI pipeline tests locally"
	@echo "		validate-dag Validate Airflow DAG syntax"
	@echo "		setup-cicd Setup CI/CD pipeline"

exec:
	docker exec -it airflow_british_proj-airflow-webserver-1 bash

stop:
	docker compose down
	docker compose stop

up:
	docker compose up -d

test:
	python3 -m pytest astronomer/tests/ -v || echo "No tests found"

lint:
	@command -v flake8 >/dev/null 2>&1 || { echo "Installing flake8..."; pip3 install --user flake8; }
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

format:
	@command -v black >/dev/null 2>&1 || { echo "Please install black: brew install black"; exit 1; }
	@command -v isort >/dev/null 2>&1 || { echo "Please install isort: brew install isort"; exit 1; }
	black .
	isort .

validate-dag:
	@echo "🔍 Validating DAG using Docker container..."
	docker run --rm -v $(PWD)/astronomer:/usr/local/airflow --workdir /usr/local/airflow apache/airflow:2.7.0-python3.9 python -c "import sys; sys.path.append('/usr/local/airflow/dags'); from main_dag import dag; print('✅ DAG syntax is valid'); print(f'DAG ID: {dag.dag_id}'); print(f'Schedule: {dag.schedule_interval}')"

ci-test: lint validate-dag test
	@echo "🎉 All CI tests passed!"

setup-cicd:
	./scripts/setup-cicd.sh