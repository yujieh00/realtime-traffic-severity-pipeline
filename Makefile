# Convenience targets. Run `make help` to list them.

.PHONY: help install sample train test lint kafka up down logs clean docker-build docker-train docker-up docker-down

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---- Local (no Docker) ----
install:  ## Install Python dependencies
	pip install -r requirements.txt

sample:  ## Generate the synthetic sample dataset
	python src/generate_sample_data.py

train:  ## Train + tune the severity model (writes to models/)
	python src/train_model.py

test:  ## Run the unit tests
	pytest -v

lint:  ## Lint with ruff
	ruff check src tests

# ---- Kafka only (local scripts against a containerised broker) ----
kafka:  ## Start just the Kafka broker
	docker compose up -d kafka

# ---- Full pipeline in Docker ----
docker-build:  ## Build the application image
	docker compose build

docker-train:  ## Generate data + train the model in a container
	docker compose run --rm trainer

docker-up:  ## Start the full pipeline (kafka + producer + streaming + dashboard)
	docker compose up -d kafka producer streaming dashboard

docker-down:  ## Stop and remove all containers
	docker compose down

logs:  ## Follow the streaming job logs
	docker compose logs -f streaming

clean:  ## Remove generated runtime artifacts
	rm -rf checkpoint output spark-warehouse metastore_db derby.log \
	       src/__pycache__ tests/__pycache__ .pytest_cache .ruff_cache
