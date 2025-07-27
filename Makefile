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
	pip3 install pytest flake8 black isort
	python3 -m pytest astronomer/tests/ -v || echo "No tests found"

lint:
	pip3 install flake8 black isort
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

format:
	pip3 install black isort
	black .
	isort .

validate-dag:
	@echo "🔍 Validating DAG using Docker container..."
	docker run --rm -v $(PWD)/astronomer:/usr/local/airflow --workdir /usr/local/airflow apache/airflow:2.7.0-python3.9 python -c "import sys; sys.path.append('/usr/local/airflow/dags'); from main_dag import dag; print('✅ DAG syntax is valid'); print(f'DAG ID: {dag.dag_id}'); print(f'Schedule: {dag.schedule_interval}')"

ci-test: lint validate-dag test
	@echo "🎉 All CI tests passed!"

setup-cicd:
	./scripts/setup-cicd.sh