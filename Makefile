# Makefile for Marketing Analytics Pipeline (Simplified Orchestration)

# Define the default target when you just run 'make'
.DEFAULT_GOAL := help

# --- Targets ---

# Display help message
help:
	@echo "Usage:"
	@echo "  make install-requirements - Install Python dependencies from requirements.txt"
	@echo "  make generate-data        - Run the script to generate mock data"
	@echo "  make run-pipeline         - Execute the main marketing pipeline script using Prefect"
	@echo "  make run-dashboard        - Run the Streamlit analytics dashboard"
	@echo "  make prefect-ui           - Start the Prefect UI (Orion server)"
	@echo "  make deploy-prefect       - Build and apply the Prefect deployment"
	@echo "  make start-agent          - Start the Prefect agent to execute flow runs"

# Install Python dependencies from requirements.txt
install-requirements:
	@echo "Installing Python dependencies..."
	pip install -r requirements.txt
	@echo "Dependencies installed."

# Generate mock data
generate-data:
	@echo "Generating mock data..."
	python scripts/generate_mock_data.py
	@echo "Mock data generation complete."

# Execute the main marketing pipeline script
run-pipeline:
	@echo "Running the marketing pipeline..."
	# Execute run_pipeline as a module from the project root
	PREFECT_PROFILE=default python -m scripts.run_pipeline
	@echo "Marketing pipeline execution finished."

# Run the Streamlit analytics dashboard
run-dashboard:
	@echo "Running the Streamlit dashboard..."
	streamlit run src/analytics/dashboard.py
	@echo "Streamlit dashboard started. Check your browser."

test:
	pytest .

# Start the Prefect UI (Orion server)
prefect-ui:
	@echo "Starting the Prefect UI..."
	prefect server start

# Build and apply the Prefect deployment
deploy-prefect: install-requirements
	@echo "Building and applying Prefect deployment..."
	# Build the deployment configuration and apply it to the Prefect server
	prefect deployment build ./scripts/run_pipeline.py:marketing_pipeline_flow --name marketing-pipeline-local --apply
	@echo "Prefect deployment applied."

# Start the Prefect agent to execute flow runs
start-agent: install-requirements
	@echo "Starting the Prefect agent..."
	# Start an agent that listens to the 'default' work queue in the 'default-agent-pool'
	prefect agent start --pool default-agent-pool --work-queue default
	@echo "Prefect agent started."

# Phony targets - prevent make from confusing targets with files of the same name
.PHONY: help install-requirements generate-data run-pipeline run-dashboard prefect-ui deploy-prefect start-agent
