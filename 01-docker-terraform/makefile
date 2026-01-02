#------- Load Environment Variables -------
# Load environment variables from .env if it exists
ifneq ("$(wildcard .env)","")
    include .env
    export $(shell sed 's/=.*//' .env)
endif

#------- Paths -------
PYTHON=../.dtc-env/bin/python
INGEST_SCRIPT=docker-sql/main.py

.PHONY: help up down ingest clean

#------- Help -------
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

#------- Docker Services -------
up: ## Start database services (Postgres + pgAdmin)
	docker-compose -f docker-compose.yml up -d

down: ## Stop database services
	docker-compose -f docker-compose.yml down

reset: ## Stop services, remove volumes, and start fresh
	docker-compose -f docker-compose.yml down -v
	docker-compose -f docker-compose.yml up -d
	@echo "Docker services have been reset."



#------- Data Ingestion -------
ingest: ## Run data ingestion
	@if [ -z "$(TABLE)" ]; then echo "Error: TABLE is required. Example: make ingest TABLE=green_tripdata_2019_10"; exit 1; fi
	@# Add docker-sql directory to PYTHONPATH to allow module imports
	export PYTHONPATH="${PYTHONPATH}:$(shell pwd)/docker-sql"; \
	$(PYTHON) $(INGEST_SCRIPT) --table_name=$(TABLE)


#------- Cleanup -------
clean: ## Remove temporary CSV/Parquet files and logs
	rm -f docker-sql/*.csv
	rm -f docker-sql/*.parquet
	rm -f ingestion.log
	@echo "Temporary files cleaned."
