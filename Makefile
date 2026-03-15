.PHONY: build up down logs submit retrain test dbt dbt-test
.DEFAULT_GOAL := help

# Colors for terminal output
YELLOW=\033[1;33m
GREEN=\033[0;32m
NC=\033[0m

help:
	@echo "Fraud Detection Data Warehouse - Management Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make build         Rebuild all Docker images (Flink, Airflow)"
	@echo "  make up            Start all core services (Kafka, MSSQL, Redis, Postgres)"
	@echo "  make start-flink   Start Flink cluster"
	@echo "  make start-airflow Start Airflow cluster"
	@echo "  make down          Stop and remove all containers"
	@echo "  make restart       Restart all services"
	@echo "  make logs          Tail logs for all core services"
	@echo ""
	@echo "Pipelines:"
	@echo "  make setup         Run initial setup (Kafka topics, Debezium, Snowflake)"
	@echo "  make submit        Submit real-time Flink enrichment job"
	@echo "  make retrain       Re-train the ML fraud model"
	@echo ""
	@echo "DBT:"
	@echo "  make dbt           Run dbt models in Airflow container"
	@echo "  make dbt-test      Run dbt tests in Airflow container"
	@echo ""
	@echo "Testing:"
	@echo "  make test          Run all pytest tests locally"

build:
	@echo "${YELLOW}Rebuilding Docker images...${NC}"
	docker compose -f docker/docker-compose-flink.yml build
	docker compose -f docker/docker-compose-airflow.yml build

up:
	@echo "${YELLOW}Starting core base services...${NC}"
	docker compose -f docker/docker-compose.base.yml up -d 2>/dev/null || \
	docker compose -f docker/docker-compose-cdc.yml up -d

start-flink:
	@echo "${YELLOW}Starting Flink cluster...${NC}"
	docker compose -f docker/docker-compose-flink.yml up -d

start-airflow:
	@echo "${YELLOW}Starting Airflow...${NC}"
	docker compose -f docker/docker-compose-airflow.yml up -d

down:
	@echo "${YELLOW}Stopping all services...${NC}"
	docker compose -f docker/docker-compose-flink.yml down
	docker compose -f docker/docker-compose-airflow.yml down
	docker compose -f docker/docker-compose-cdc.yml down

restart: down up start-flink start-airflow

logs:
	docker compose -f docker/docker-compose-cdc.yml logs -f --tail=50

setup:
	@echo "${YELLOW}Running initial setup...${NC}"
	bash scripts/setup_debezium.sh
	bash scripts/create_kafka_topics.sh
	@echo "You may need to run Snowflake setup manually: scripts/snowflake_s3_setup.sql"

submit:
	@echo "${YELLOW}Submitting Flink Job...${NC}"
	bash scripts/submit_flink_job.sh

retrain:
	@echo "${YELLOW}Retraining ML Model...${NC}"
	docker exec flink-jobmanager python3 /opt/flink-apps/train_fraud_model.py --mode unsupervised --config /opt/flink-apps/config.ini --output /opt/flink-apps/models/fraud_model.joblib

dbt:
	@echo "${YELLOW}Running dbt run...${NC}"
	docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt_fraud_dwh && dbt run"

dbt-test:
	@echo "${YELLOW}Running dbt test...${NC}"
	docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt_fraud_dwh && dbt test"

test:
	@echo "${YELLOW}Running all tests...${NC}"
	python -m pytest tests/
