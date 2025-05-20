# Makefile for Marketing Analytics Pipeline (Simplified Orchestration)

# Define the default target when you just run 'make'
.DEFAULT_GOAL := help
PROJECT_ROOT := $(shell pwd)

# --- Targets ---

# Display help message
help:
	@echo "Usage:"
	@echo "  make generate-data        - Run the script to generate mock data"
	@echo "  make run-dashboard        - Run the Streamlit analytics dashboard"
	@echo "  make up                   - Run the full service"

# Run the service
up:
	@echo "Starting prefect-ui, prefect-agent, and file watchdog with docker-compose"
	docker-compose up -d

# Stop the service
down:
	@echo "Stopping prefect-ui, prefect-agent, and file watchdog with docker-compose"
	docker-compose down

# Generate mock data
generate-data:
	@echo "Generating mock data..."
	python scripts/generate_mock_data.py
	@echo "Mock data generation complete."

# Run the Streamlit analytics dashboard
run-dashboard:
	@echo "Running the Streamlit dashboard..."
	streamlit run src/analytics/dashboard.py
	@echo "Streamlit dashboard started. Check your browser."

test:
	pytest .

clean:
	@echo "Cleaning generated files..."
	rm -rf data/processed/*
	rm -rf data/reports/*
	# Optional: remove __pycache__ and .pytest_cache
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	@echo "Cleaned."

# Phony targets - prevent make from confusing targets with files of the same name
.PHONY: help up generate-data run-dashboard clean
