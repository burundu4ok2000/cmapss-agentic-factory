# ==============================================================================
# C-MAPSS Factory 4.0: Central Control Hub (Makefile)
# ==============================================================================

.PHONY: setup init-db infra-up infra-down terraform-apply seed-lake run-pipeline lint help

# Default project variables (can be overridden)
PROJECT_ID ?= de-zoomcamp-2026-485615

help:
	@echo "C-MAPSS Agentic Factory 4.0 Management Commands:"
	@echo "  setup            Install local dependencies (Go, uv, Python)"
	@echo "  init-db          Initialize SQLite DB (Parses NASA .txt -> blueprints.sqlite)"
	@echo "  infra-up         Start local infrastructure (Redpanda, Dashboard) via Docker"
	@echo "  infra-down       Stop all local containers"
	@echo "  terraform-apply  Deploy GCP resources (GCS, BigQuery)"
	@echo "  seed-lake        Upload a valid Parquet seed to GCS to initialize DWH"
	@echo "  run-pipeline     Show instructions to start simulation and streaming"

setup:
	@echo "Installing dependencies..."
	go mod tidy
	uv pip install -r streaming/requirements.txt
	uv pip install -r dashboard/requirements.txt

init-db:
	@echo "Initializing local blueprints.sqlite database..."
	uv run python3 scripts/init_db.py

infra-up:
	docker compose up -d
	@echo "Infrastructure is up! Dashboard: http://localhost:8501, Console: http://localhost:8080"

infra-down:
	docker compose down

terraform-apply:
	cd terraform && terraform apply -auto-approve -var="project=$(PROJECT_ID)"

seed-lake:
	@echo "Generating valid Parquet seed and uploading to GCS..."
	uv run --with pyarrow --with pandas python3 -c \
		"import pandas as pd; from datetime import datetime; \
		df = pd.DataFrame([{'timestamp': datetime.now(), 'unit_number': 1, 'time_cycles': 1, 'htBleed': 0.0, 'T2': 518.67, 'T50': 641.82, 'P30': 554.43, 'Nf': 2388.0, 'Nc': 8138.62, 'phi': 0.0, 'is_corrupted': False, 'corruption_reason': []}]); \
		df.to_parquet('seed.parquet')"
	gcloud storage cp seed.parquet gs://cmapss-datalake-bucket/telemetry/seed.parquet
	@rm seed.parquet

run-pipeline:
	@echo "----------------------------------------------------------------------"
	@echo "TO START THE FULL DATA FLOW:"
	@echo "1. Initialize the Local Database (ETL Part):"
	@echo "   make init-db"
	@echo "2. Run the Go Simulator:"
	@echo "   go run cmd/simulator/main.go"
	@echo "3. In a separate terminal, run the PySpark Stream Processor:"
	@echo "   python3 streaming/consumer.py"
	@echo "4. Visit the Streamlit Dashboard: http://localhost:8501"
	@echo "----------------------------------------------------------------------"

lint:
	ruff check streaming/ dashboard/
