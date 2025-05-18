# Makefile for Marketing Analytics Pipeline (Simplified Orchestration)

# Define the default target when you just run 'make'
.DEFAULT_GOAL := help

# --- Targets ---

# Display help message
help:
	@echo "Usage:"
	@echo "  make install-requirements - Install Python dependencies from requirements.txt"
	@echo "  make generate-data        - Run the script to generate mock data"
	@echo "  make run-pipeline         - Execute the main marketing pipeline script"
	@echo "  make run-dashboard        - Run the Streamlit analytics dashboard"
	@echo ""
	# You could add more targets here later, e.g., for running tests

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
	python -m scripts.run_pipeline
	@echo "Marketing pipeline execution finished."

# Run the Streamlit analytics dashboard
run-dashboard:
	@echo "Running the Streamlit dashboard..."
	streamlit run src/analytics/dashboard.py
	@echo "Streamlit dashboard started. Check your browser."

test:
	pytest .


# Phony targets - prevent make from confusing targets with files of the same name
.PHONY: help install-requirements generate-data run-pipeline run-dashboard
